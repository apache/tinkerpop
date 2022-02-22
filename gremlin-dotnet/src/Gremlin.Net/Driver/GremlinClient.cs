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
using System.Net.WebSockets;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO;
using Gremlin.Net.Structure.IO.GraphSON;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Provides a mechanism for submitting Gremlin requests to one Gremlin Server.
    /// </summary>
    public class GremlinClient : IGremlinClient
    {
        private readonly ConnectionPool _connectionPool;

        /// <summary>
        ///     Initializes a new instance of the <see cref="GremlinClient" /> class for the specified Gremlin Server.
        /// </summary>
        /// <param name="gremlinServer">The <see cref="GremlinServer" /> the requests should be sent to.</param>
        /// <param name="graphSONReader">A <see cref="GraphSONReader" /> instance to read received GraphSON data.</param>
        /// <param name="graphSONWriter">a <see cref="GraphSONWriter" /> instance to write GraphSON data.</param>
        /// <param name="connectionPoolSettings">The <see cref="ConnectionPoolSettings" /> for the connection pool.</param>
        /// <param name="webSocketConfiguration">
        ///     A delegate that will be invoked with the <see cref="ClientWebSocketOptions" />
        ///     object used to configure WebSocket connections.
        /// </param>
        /// <param name="sessionId">The session Id if Gremlin Client in session mode, defaults to null as session-less Client.</param>
        [Obsolete("This constructor is obsolete. Use the constructor that takes a IMessageSerializer instead.")]
        public GremlinClient(GremlinServer gremlinServer, GraphSONReader graphSONReader, GraphSONWriter graphSONWriter,
            ConnectionPoolSettings connectionPoolSettings = null,
            Action<ClientWebSocketOptions> webSocketConfiguration = null, string sessionId = null)
            : this(gremlinServer, graphSONReader, graphSONWriter, SerializationTokens.GraphSON3MimeType,
                connectionPoolSettings, webSocketConfiguration, sessionId)
        {
        }
        
        /// <summary>
        ///     Initializes a new instance of the <see cref="GremlinClient" /> class for the specified Gremlin Server.
        /// </summary>
        /// <param name="gremlinServer">The <see cref="GremlinServer" /> the requests should be sent to.</param>
        /// <param name="graphSONReader">A <see cref="GraphSONReader" /> instance to read received GraphSON data.</param>
        /// <param name="graphSONWriter">a <see cref="GraphSONWriter" /> instance to write GraphSON data.</param>
        /// <param name="mimeType">The GraphSON version mime type, defaults to latest supported by the server.</param>
        /// <param name="connectionPoolSettings">The <see cref="ConnectionPoolSettings" /> for the connection pool.</param>
        /// <param name="webSocketConfiguration">
        ///     A delegate that will be invoked with the <see cref="ClientWebSocketOptions" />
        ///     object used to configure WebSocket connections.
        /// </param>
        /// <param name="sessionId">The session Id if Gremlin Client in session mode, defaults to null as session-less Client.</param>
        [Obsolete("This constructor is obsolete. Use the constructor that takes a IMessageSerializer instead.")]
        public GremlinClient(GremlinServer gremlinServer, GraphSONReader graphSONReader, GraphSONWriter graphSONWriter,
            string mimeType, ConnectionPoolSettings connectionPoolSettings = null,
            Action<ClientWebSocketOptions> webSocketConfiguration = null, string sessionId = null)
        {
            IMessageSerializer messageSerializer;
            switch (mimeType)
            {
                case SerializationTokens.GraphSON3MimeType:
                    VerifyGraphSONArgumentTypeForMimeType<GraphSON3Reader>(graphSONReader, nameof(graphSONReader),
                        mimeType);
                    VerifyGraphSONArgumentTypeForMimeType<GraphSON3Writer>(graphSONWriter, nameof(graphSONWriter),
                        mimeType);
                    messageSerializer = new GraphSON3MessageSerializer(
                        (GraphSON3Reader) graphSONReader ?? new GraphSON3Reader(),
                        (GraphSON3Writer) graphSONWriter ?? new GraphSON3Writer());
                    break;
                case SerializationTokens.GraphSON2MimeType:
                    VerifyGraphSONArgumentTypeForMimeType<GraphSON2Reader>(graphSONReader, nameof(graphSONReader),
                        mimeType);
                    VerifyGraphSONArgumentTypeForMimeType<GraphSON2Writer>(graphSONWriter, nameof(graphSONWriter),
                        mimeType);
                    messageSerializer = new GraphSON2MessageSerializer(
                        (GraphSON2Reader) graphSONReader ?? new GraphSON2Reader(),
                        (GraphSON2Writer) graphSONWriter ?? new GraphSON2Writer());
                    break;
                default:
                    throw new ArgumentException(nameof(mimeType), $"{mimeType} not supported");
            }

            var connectionFactory =
                new ConnectionFactory(gremlinServer, messageSerializer,
                    new WebSocketSettings { WebSocketConfigurationCallback = webSocketConfiguration }, sessionId);

            // make sure one connection in pool as session mode
            if (!string.IsNullOrEmpty(sessionId))
            {
                if (connectionPoolSettings != null)
                {
                    if (connectionPoolSettings.PoolSize != 1)
                        throw new ArgumentOutOfRangeException(nameof(connectionPoolSettings), "PoolSize must be 1 in session mode!");
                }
                else
                {
                    connectionPoolSettings = new ConnectionPoolSettings {PoolSize = 1};
                }
            }
            _connectionPool =
                new ConnectionPool(connectionFactory, connectionPoolSettings ?? new ConnectionPoolSettings());
        }

        private static void VerifyGraphSONArgumentTypeForMimeType<T>(object argument, string argumentName,
            string mimeType)
        {
            if (argument != null && !(argument is T))
            {
                throw new ArgumentException(
                    $"{argumentName} is not a {typeof(T).Name} but the mime type is: {mimeType}", argumentName);
            }
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GremlinClient" /> class for the specified Gremlin Server.
        /// </summary>
        /// <param name="gremlinServer">The <see cref="GremlinServer" /> the requests should be sent to.</param>
        /// <param name="messageSerializer">
        ///     A <see cref="IMessageSerializer" /> instance to serialize messages sent to and received
        ///     from the server.
        /// </param>
        /// <param name="connectionPoolSettings">The <see cref="ConnectionPoolSettings" /> for the connection pool.</param>
        /// <param name="webSocketConfiguration">
        ///     A delegate that will be invoked with the <see cref="ClientWebSocketOptions" />
        ///     object used to configure WebSocket connections.
        /// </param>
        /// <param name="sessionId">The session Id if Gremlin Client in session mode, defaults to null as session-less Client.</param>
        /// <param name="disableCompression">
        ///     Whether to disable compression. Compression is only supported since .NET 6.
        ///     There it is also enabled by default.
        ///
        ///     Note that compression might make your application susceptible to attacks like CRIME/BREACH. Compression
        ///     should therefore be turned off if your application sends sensitive data to the server as well as data
        ///     that could potentially be controlled by an untrusted user.
        /// </param>
        public GremlinClient(GremlinServer gremlinServer, IMessageSerializer messageSerializer = null,
            ConnectionPoolSettings connectionPoolSettings = null,
            Action<ClientWebSocketOptions> webSocketConfiguration = null, string sessionId = null,
            bool disableCompression = false)
        {
            messageSerializer ??= new GraphSON3MessageSerializer();
            var webSocketSettings = new WebSocketSettings
            {
                WebSocketConfigurationCallback = webSocketConfiguration
#if NET6_0_OR_GREATER
                , UseCompression = !disableCompression
#endif
            };
            var connectionFactory =
                new ConnectionFactory(gremlinServer, messageSerializer, webSocketSettings, sessionId);

            // make sure one connection in pool as session mode
            if (!string.IsNullOrEmpty(sessionId))
            {
                if (connectionPoolSettings != null)
                {
                    if (connectionPoolSettings.PoolSize != 1)
                        throw new ArgumentOutOfRangeException(nameof(connectionPoolSettings),
                            "PoolSize must be 1 in session mode!");
                }
                else
                {
                    connectionPoolSettings = new ConnectionPoolSettings {PoolSize = 1};
                }
            }
            _connectionPool =
                new ConnectionPool(connectionFactory, connectionPoolSettings ?? new ConnectionPoolSettings());
        }

        /// <summary>
        ///     Gets the number of open connections.
        /// </summary>
        public int NrConnections => _connectionPool.NrConnections;

        /// <inheritdoc />
        public async Task<ResultSet<T>> SubmitAsync<T>(RequestMessage requestMessage)
        {
            using var connection = _connectionPool.GetAvailableConnection();
            return await connection.SubmitAsync<T>(requestMessage).ConfigureAwait(false);
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
                    _connectionPool?.Dispose();
                _disposed = true;
            }
        }

        #endregion
    }
}