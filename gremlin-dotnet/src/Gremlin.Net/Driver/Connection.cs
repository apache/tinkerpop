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
using Gremlin.Net.Structure.IO;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     HTTP-based connection that sends requests via HTTP POST to Gremlin Server.
    /// </summary>
    internal class Connection : IDisposable
    {
        private const string GraphBinaryMimeType = SerializationTokens.GraphBinary4MimeType;

        private readonly HttpClient _httpClient;
        private readonly Uri _uri;
        private readonly IMessageSerializer _serializer;
        private readonly ConnectionSettings _settings;
        // Interceptor slot reserved for future spec
        // private readonly IReadOnlyList<Func<HttpRequestMessage, Task>> _interceptors;

        /// <summary>
        ///     Creates a new HTTP connection. The <see cref="HttpClient"/> is backed by
        ///     SocketsHttpHandler which manages its own TCP connection pool internally,
        ///     so a single <see cref="Connection"/> instance handles concurrent requests efficiently.
        /// </summary>
        public Connection(Uri uri, IMessageSerializer serializer,
            ConnectionSettings settings)
        {
            _uri = uri;
            _serializer = serializer;
            _settings = settings;

#if NET6_0_OR_GREATER
            var handler = new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = settings.IdleConnectionTimeout,
                MaxConnectionsPerServer = settings.MaxConnectionsPerServer,
                ConnectTimeout = settings.ConnectionTimeout,
                KeepAlivePingTimeout = settings.KeepAliveInterval,
            };
            _httpClient = new HttpClient(handler);
#else
            _httpClient = new HttpClient();
            _httpClient.Timeout = settings.ConnectionTimeout;
#endif
        }

        /// <summary>
        ///     Constructor that accepts a pre-configured HttpClient (for testing).
        /// </summary>
        internal Connection(Uri uri, IMessageSerializer serializer,
            ConnectionSettings settings, HttpClient httpClient)
        {
            _uri = uri;
            _serializer = serializer;
            _settings = settings;
            _httpClient = httpClient;
        }

        public async Task<ResultSet<T>> SubmitAsync<T>(RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            var requestBytes = await _serializer.SerializeMessageAsync(requestMessage, cancellationToken)
                .ConfigureAwait(false);

            using var content = new ByteArrayContent(requestBytes);
            content.Headers.ContentType = new MediaTypeHeaderValue(GraphBinaryMimeType);

            using var httpRequest = new HttpRequestMessage(HttpMethod.Post, _uri);
            httpRequest.Content = content;
            httpRequest.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(GraphBinaryMimeType));

            if (_settings.EnableCompression)
            {
                httpRequest.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
            }

            if (_settings.EnableUserAgentOnConnect)
            {
                httpRequest.Headers.TryAddWithoutValidation("User-Agent", Utils.UserAgent);
            }

            if (_settings.BulkResults)
            {
                httpRequest.Headers.Add("bulkResults", "true");
            }

            // Future: apply interceptors here

            using var response = await _httpClient.SendAsync(httpRequest, cancellationToken)
                .ConfigureAwait(false);

            // If the HTTP status indicates an error and the response is not GraphBinary,
            // attempt to read the body as a text error message. The server may respond with
            // a JSON body containing an error field, or plain text. Only when the Content-Type
            // is GraphBinary do we fall through to normal deserialization so the status footer
            // in the GB4 response can surface the application-level error.
            if (!response.IsSuccessStatusCode &&
                response.Content.Headers.ContentType?.MediaType != GraphBinaryMimeType)
            {
                var errorBody = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                // Try to extract the "message" field from a JSON error response
                var errorMessage = TryExtractJsonError(errorBody)
                    ?? $"Gremlin Server returned HTTP {(int)response.StatusCode}: {errorBody}";

                throw new HttpRequestException(errorMessage);
            }

            var responseBytes = await ReadResponseBytesAsync(response).ConfigureAwait(false);

            var responseMessage = await _serializer.DeserializeMessageAsync(responseBytes, cancellationToken)
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
            return new ResultSet<T>(
                responseMessage.Result.Cast<T>().ToList(),
                new Dictionary<string, object>());
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
