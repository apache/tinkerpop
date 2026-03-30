#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure.IO;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     HTTP-based connection that sends requests via HTTP POST to Gremlin Server.
    /// </summary>
    internal class Connection : IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly Uri _uri;
        private readonly IMessageSerializer? _requestSerializer;
        private readonly IMessageSerializer _responseSerializer;
        private readonly ConnectionSettings _settings;
        private readonly IReadOnlyList<Func<HttpRequestContext, Task>> _interceptors;

        /// <summary>
        ///     Creates a new HTTP connection. The <see cref="HttpClient"/> is backed by
        ///     SocketsHttpHandler which manages its own TCP connection pool internally,
        ///     so a single <see cref="Connection"/> instance handles concurrent requests efficiently.
        /// </summary>
        /// <param name="uri">The Gremlin Server URI.</param>
        /// <param name="requestSerializer">
        ///     The serializer for outgoing requests. When non-null, the request body is serialized
        ///     to <c>byte[]</c> before interceptors run and the <c>Content-Type</c> header is set
        ///     automatically. When <c>null</c>, the body is passed as a <see cref="RequestMessage"/>
        ///     and an interceptor is responsible for serializing it to <c>byte[]</c> and setting
        ///     <c>Content-Type</c>. This follows the Python driver's <c>request_serializer=None</c>
        ///     pattern.
        /// </param>
        /// <param name="responseSerializer">The serializer for incoming responses (always required).</param>
        /// <param name="settings">Connection settings.</param>
        /// <param name="interceptors">Optional request interceptors.</param>
        public Connection(Uri uri, IMessageSerializer? requestSerializer,
            IMessageSerializer responseSerializer,
            ConnectionSettings settings,
            IReadOnlyList<Func<HttpRequestContext, Task>>? interceptors = null)
        {
            _uri = uri;
            _requestSerializer = requestSerializer;
            _responseSerializer = responseSerializer;
            _settings = settings;
            _interceptors = interceptors ?? Array.Empty<Func<HttpRequestContext, Task>>();

#if NET6_0_OR_GREATER
            var handler = new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = settings.IdleConnectionTimeout,
                MaxConnectionsPerServer = settings.MaxConnectionsPerServer,
                ConnectTimeout = settings.ConnectionTimeout,
                KeepAlivePingTimeout = settings.KeepAliveInterval,
            };
            if (settings.SkipCertificateValidation)
            {
                handler.SslOptions = new System.Net.Security.SslClientAuthenticationOptions
                {
                    RemoteCertificateValidationCallback = (_, _, _, _) => true,
                };
            }
            _httpClient = new HttpClient(handler);
#else
            _httpClient = new HttpClient();
            _httpClient.Timeout = settings.ConnectionTimeout;
#endif
        }

        /// <summary>
        ///     Constructor that accepts a pre-configured HttpClient (for testing).
        /// </summary>
        internal Connection(Uri uri, IMessageSerializer? requestSerializer,
            IMessageSerializer responseSerializer,
            ConnectionSettings settings, HttpClient httpClient,
            IReadOnlyList<Func<HttpRequestContext, Task>>? interceptors = null)
        {
            _uri = uri;
            _requestSerializer = requestSerializer;
            _responseSerializer = responseSerializer;
            _settings = settings;
            _httpClient = httpClient;
            _interceptors = interceptors ?? Array.Empty<Func<HttpRequestContext, Task>>();
        }

        public async Task<ResultSet<T>> SubmitAsync<T>(RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            // Build HttpRequestContext with default headers
            var headers = new Dictionary<string, string>();
            headers["Accept"] = _responseSerializer.MimeType;

            if (_settings.EnableCompression)
            {
                headers["Accept-Encoding"] = "deflate";
            }

            if (_settings.EnableUserAgentOnConnect)
            {
                headers["User-Agent"] = Utils.UserAgent;
            }

            if (_settings.BulkResults)
            {
                headers["bulkResults"] = "true";
            }

            object body;
            if (_requestSerializer != null)
            {
                // Default path: serialize before interceptors
                var requestBytes = await _requestSerializer.SerializeMessageAsync(requestMessage, cancellationToken)
                    .ConfigureAwait(false);
                body = requestBytes;
                headers["Content-Type"] = _requestSerializer.MimeType;
            }
            else
            {
                // Interceptor-managed path: body is RequestMessage, interceptor must serialize
                body = requestMessage;
            }

            var context = new HttpRequestContext("POST", _uri, headers, body);

            // Apply interceptors in order
            foreach (var interceptor in _interceptors)
            {
                await interceptor(context).ConfigureAwait(false);
            }

            // Convert HttpRequestContext to HttpRequestMessage
            using var httpRequest = new HttpRequestMessage(new HttpMethod(context.Method), context.Uri);

            if (context.Body is byte[] bodyBytes)
            {
                httpRequest.Content = new ByteArrayContent(bodyBytes);
            }
            else if (context.Body is HttpContent httpContent)
            {
                httpRequest.Content = httpContent;
            }
            else
            {
                throw new InvalidOperationException(
                    "Request body must be byte[] or HttpContent after all interceptors complete, " +
                    "but found " + (context.Body?.GetType().Name ?? "null") +
                    ". Either provide a requestSerializer or add an interceptor " +
                    "that serializes the RequestMessage.");
            }

            foreach (var header in context.Headers)
            {
                // Content-Type must be set on the content headers, not the request headers
                if (string.Equals(header.Key, "Content-Type", StringComparison.OrdinalIgnoreCase))
                {
                    httpRequest.Content.Headers.ContentType = new MediaTypeHeaderValue(header.Value);
                }
                else
                {
                    httpRequest.Headers.TryAddWithoutValidation(header.Key, header.Value);
                }
            }

            using var response = await _httpClient.SendAsync(httpRequest, cancellationToken)
                .ConfigureAwait(false);

            // If the HTTP status indicates an error and the response is not GraphBinary,
            // attempt to read the body as a text error message. The server may respond with
            // a JSON body containing an error field, or plain text. Only when the Content-Type
            // is GraphBinary do we fall through to normal deserialization so the status footer
            // in the GB4 response can surface the application-level error.
            if (!response.IsSuccessStatusCode &&
                response.Content.Headers.ContentType?.MediaType != _responseSerializer.MimeType)
            {
                var errorBody = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                // Try to extract the "message" field from a JSON error response
                var errorMessage = TryExtractJsonError(errorBody)
                    ?? $"Gremlin Server returned HTTP {(int)response.StatusCode}: {errorBody}";

                throw new HttpRequestException(errorMessage);
            }

            var responseBytes = await ReadResponseBytesAsync(response).ConfigureAwait(false);

            var responseMessage = await _responseSerializer.DeserializeMessageAsync(responseBytes, cancellationToken)
                .ConfigureAwait(false);

            return BuildResultSet<T>(responseMessage);
        }

        private static async Task<byte[]> ReadResponseBytesAsync(HttpResponseMessage response)
        {
            using var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            if (response.Content.Headers.ContentEncoding.Contains("deflate"))
            {
                using var deflateStream = new DeflateStream(stream, CompressionMode.Decompress);
                using var ms = new MemoryStream();
                await deflateStream.CopyToAsync(ms).ConfigureAwait(false);
                return ms.ToArray();
            }
            using var memoryStream = new MemoryStream();
            await stream.CopyToAsync(memoryStream).ConfigureAwait(false);
            return memoryStream.ToArray();
        }

        private static ResultSet<T> BuildResultSet<T>(ResponseMessage<List<object>> responseMessage)
        {
            var results = typeof(T) == typeof(Traverser)
                ? responseMessage.Result
                    .Select(item => item is Traverser ? item : (object)new Traverser(item))
                    .Cast<T>().ToList()
                : responseMessage.Result.Cast<T>().ToList();

            return new ResultSet<T>(results, new Dictionary<string, object>());
        }

        /// <summary>
        ///     Attempts to extract an error message from a JSON response body.
        ///     The server sometimes responds with a JSON object containing a "message" field
        ///     even when it cannot produce a GraphBinary response.
        /// </summary>
        private static string? TryExtractJsonError(string body)
        {
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(body);
                if (doc.RootElement.TryGetProperty("message", out var messageProp))
                {
                    return messageProp.GetString();
                }
            }
            catch
            {
                // Not valid JSON — fall through to raw body
            }
            return null;
        }

        #region IDisposable Support

        private bool _disposed;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _httpClient?.Dispose();
                }
                _disposed = true;
            }
        }

        #endregion
    }
}
