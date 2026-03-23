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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Provides a mechanism for submitting Gremlin requests to one Gremlin Server.
    /// </summary>
    public class GremlinClient : IGremlinClient
    {
        private readonly Connection _connection;

        internal ILoggerFactory LoggerFactory { get; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GremlinClient" /> class for the specified Gremlin Server.
        /// </summary>
        /// <param name="gremlinServer">The <see cref="GremlinServer" /> the requests should be sent to.</param>
        /// <param name="requestSerializer">
        ///     A <see cref="IMessageSerializer" /> instance to serialize outgoing request messages.
        ///     When <c>null</c>, the request body is passed as a <see cref="RequestMessage"/> to
        ///     interceptors, and an interceptor must serialize it to <c>byte[]</c> and set the
        ///     <c>Content-Type</c> header. This follows the Python driver's
        ///     <c>request_serializer=None</c> pattern.
        /// </param>
        /// <param name="responseSerializer">
        ///     A <see cref="IMessageSerializer" /> instance to deserialize incoming response messages.
        ///     Always required.
        /// </param>
        /// <param name="connectionSettings">The <see cref="ConnectionSettings" /> for the HTTP connection.</param>
        /// <param name="loggerFactory">A factory to create loggers. If not provided, then nothing will be logged.</param>
        /// <param name="interceptors">
        ///     An optional list of request interceptors. Each interceptor receives a mutable
        ///     <see cref="HttpRequestContext" /> and can modify headers, body, URI, and method
        ///     before the request is sent.
        /// </param>
        public GremlinClient(GremlinServer gremlinServer, IMessageSerializer? requestSerializer,
            IMessageSerializer responseSerializer,
            ConnectionSettings? connectionSettings = null,
            ILoggerFactory? loggerFactory = null,
            IReadOnlyList<Func<HttpRequestContext, Task>>? interceptors = null)
        {
            connectionSettings ??= new ConnectionSettings();
            LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;

            _connection = new Connection(
                gremlinServer.Uri,
                requestSerializer,
                responseSerializer,
                connectionSettings,
                interceptors);

            var logger = LoggerFactory.CreateLogger<GremlinClient>();
            logger.InitializedHttpConnection(gremlinServer.Uri);
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GremlinClient" /> class with a single
        ///     serializer used for both request serialization and response deserialization.
        ///     This is the backward-compatible convenience constructor.
        /// </summary>
        /// <param name="gremlinServer">The <see cref="GremlinServer" /> the requests should be sent to.</param>
        /// <param name="messageSerializer">
        ///     A <see cref="IMessageSerializer" /> instance used for both request serialization and
        ///     response deserialization. Defaults to <see cref="GraphBinary4MessageSerializer"/>.
        /// </param>
        /// <param name="connectionSettings">The <see cref="ConnectionSettings" /> for the HTTP connection.</param>
        /// <param name="loggerFactory">A factory to create loggers. If not provided, then nothing will be logged.</param>
        /// <param name="interceptors">
        ///     An optional list of request interceptors.
        /// </param>
        public GremlinClient(GremlinServer gremlinServer, IMessageSerializer? messageSerializer = null,
            ConnectionSettings? connectionSettings = null,
            ILoggerFactory? loggerFactory = null,
            IReadOnlyList<Func<HttpRequestContext, Task>>? interceptors = null)
            : this(gremlinServer,
                  messageSerializer ?? new GraphBinary4MessageSerializer(),
                  messageSerializer ?? new GraphBinary4MessageSerializer(),
                  connectionSettings, loggerFactory, interceptors)
        {
        }

        /// <inheritdoc />
        public async Task<ResultSet<T>> SubmitAsync<T>(RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            return await _connection.SubmitAsync<T>(requestMessage, cancellationToken)
                .ConfigureAwait(false);
        }

        #region IDisposable Support

        private bool _disposed;

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///     Releases the resources used by the <see cref="GremlinClient" /> instance.
        /// </summary>
        /// <param name="disposing">Specifies whether managed resources should be released.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                    _connection?.Dispose();
                _disposed = true;
            }
        }

        #endregion
    }
}
