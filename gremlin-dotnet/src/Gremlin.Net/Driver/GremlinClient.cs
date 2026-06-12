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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure;
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
        private readonly ConcurrentDictionary<RemoteTransaction, byte> _trackedTransactions = new();

        internal ILoggerFactory LoggerFactory { get; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GremlinClient" /> class for the specified Gremlin Server.
        /// </summary>
        /// <param name="gremlinServer">The <see cref="GremlinServer" /> the requests should be sent to.</param>
        /// <param name="responseSerializer">
        ///     A <see cref="IMessageSerializer" /> instance to deserialize incoming response messages.
        ///     Defaults to <see cref="GraphBinary4MessageSerializer"/>.
        /// </param>
        /// <param name="connectionSettings">The <see cref="ConnectionSettings" /> for the HTTP connection.</param>
        /// <param name="loggerFactory">A factory to create loggers. If not provided, then nothing will be logged.</param>
        /// <param name="interceptors">
        ///     An optional list of request interceptors. Each interceptor receives a mutable
        ///     <see cref="HttpRequestContext" /> and can modify headers, body, URI, and method
        ///     before the request is sent. Interceptors that need the serialized bytes (e.g.
        ///     for payload signing) should call <see cref="HttpRequestContext.SerializeBody"/>.
        /// </param>
        /// <param name="auth">
        ///     An optional auth interceptor. As a convenience, this is appended to the end of the
        ///     interceptor list so it runs last (after any user interceptors have modified the request).
        ///     This is equivalent to including the auth interceptor as the last element of <paramref name="interceptors"/>.
        /// </param>
        /// <param name="pdtRegistry">
        ///     An optional <see cref="ProviderDefinedTypeRegistry"/> for automatic hydration of
        ///     provider-defined types.
        /// </param>
        public GremlinClient(GremlinServer gremlinServer,
            IMessageSerializer? responseSerializer = null,
            ConnectionSettings? connectionSettings = null,
            ILoggerFactory? loggerFactory = null,
            IReadOnlyList<Func<HttpRequestContext, Task>>? interceptors = null,
            Func<HttpRequestContext, Task>? auth = null,
            ProviderDefinedTypeRegistry? pdtRegistry = null)
        {
            connectionSettings ??= new ConnectionSettings();
            LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;

            var actualResponseSerializer = responseSerializer ?? new GraphBinary4MessageSerializer();

            if (pdtRegistry != null)
            {
                actualResponseSerializer.SetPdtRegistry(pdtRegistry);
            }

            // Append auth interceptor to the end of the list so it runs last.
            IReadOnlyList<Func<HttpRequestContext, Task>>? allInterceptors = interceptors;
            if (auth != null)
            {
                var list = interceptors?.ToList() ?? new List<Func<HttpRequestContext, Task>>();
                list.Add(auth);
                allInterceptors = list;
            }

            _connection = new Connection(
                gremlinServer.Uri,
                actualResponseSerializer,
                connectionSettings,
                allInterceptors);

            var logger = LoggerFactory.CreateLogger<GremlinClient>();
            logger.InitializedHttpConnection(gremlinServer.Uri);
        }

        /// <inheritdoc />
        public async Task<ResultSet<T>> SubmitAsync<T>(RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            return await _connection.SubmitAsync<T>(requestMessage, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>
        ///     Creates a new <see cref="RemoteTransaction"/> for executing operations within an explicit
        ///     server-side transaction. Transactions are short-lived and single-use: after commit
        ///     or rollback, create a new RemoteTransaction for the next unit of work.
        /// </summary>
        /// <param name="traversalSource">The traversal source alias (e.g. "g" or "gtx").</param>
        /// <returns>A new <see cref="RemoteTransaction"/>.</returns>
        public RemoteTransaction Transact(string traversalSource)
        {
            return new RemoteTransaction(this, traversalSource);
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
                {
                    // Best-effort rollback of any open transactions before closing connections
                    foreach (var kvp in _trackedTransactions)
                    {
                        try
                        {
                            if (kvp.Key.IsOpen)
                            {
                                kvp.Key.RollbackAsync().GetAwaiter().GetResult();
                            }
                        }
                        catch { }
                    }
                    _trackedTransactions.Clear();
                    _connection?.Dispose();
                }
                _disposed = true;
            }
        }

        internal void TrackTransaction(RemoteTransaction tx)
        {
            _trackedTransactions.TryAdd(tx, 0);
        }

        internal void UntrackTransaction(RemoteTransaction tx)
        {
            _trackedTransactions.TryRemove(tx, out _);
        }

        #endregion

        internal Connection Connection => _connection;
    }
}
