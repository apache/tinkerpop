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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
    internal class HttpConnection : IDisposable
    {
        private const string GraphBinaryMimeType = SerializationTokens.GraphBinary4MimeType;

        private readonly HttpClient _httpClient;
        private readonly Uri _uri;
        private readonly IMessageSerializer _serializer;
        private readonly bool _enableCompression;
        private readonly bool _enableUserAgentOnConnect;
        private readonly bool _bulkResults;
        // Interceptor slot reserved for future spec
        // private readonly IReadOnlyList<Func<HttpRequestMessage, Task>> _interceptors;

        public HttpConnection(Uri uri, IMessageSerializer serializer,
            HttpConnectionSettings settings)
        {
            _uri = uri;
            _serializer = serializer;
            _enableCompression = settings.EnableCompression;
            _enableUserAgentOnConnect = settings.EnableUserAgentOnConnect;
            _bulkResults = settings.BulkResults;

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
        internal HttpConnection(Uri uri, IMessageSerializer serializer,
            HttpConnectionSettings settings, HttpClient httpClient)
        {
            _uri = uri;
            _serializer = serializer;
            _enableCompression = settings.EnableCompression;
            _enableUserAgentOnConnect = settings.EnableUserAgentOnConnect;
            _bulkResults = settings.BulkResults;
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

            if (_enableCompression)
            {
                httpRequest.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
            }

            if (_enableUserAgentOnConnect)
            {
                httpRequest.Headers.TryAddWithoutValidation("User-Agent", Utils.UserAgent);
            }

            if (_bulkResults)
            {
                httpRequest.Headers.Add("bulkResults", "true");
            }

            // Future: apply interceptors here

            using var response = await _httpClient.SendAsync(httpRequest, cancellationToken)
                .ConfigureAwait(false);

            var responseBytes = await ReadResponseBytesAsync(response).ConfigureAwait(false);

            var responseMessage = await _serializer.DeserializeMessageAsync(responseBytes, cancellationToken)
                .ConfigureAwait(false);

            return BuildResultSet<T>(responseMessage);
        }

        private static async Task<byte[]> ReadResponseBytesAsync(HttpResponseMessage response)
        {
            var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
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
            var results = new List<T>();
            foreach (var item in responseMessage.Result)
            {
                results.Add((T)item);
            }
            return new ResultSet<T>(results, new Dictionary<string, object>());
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
