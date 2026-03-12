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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Gremlin.Net.Driver.Remote
{
    /// <summary>
    ///     A <see cref="IRemoteConnection" /> implementation for Gremlin Server.
    /// </summary>
    public class DriverRemoteConnection : IRemoteConnection, IDisposable
    {
        private readonly IGremlinClient _client;
        private readonly string _traversalSource;
        private readonly ILogger<DriverRemoteConnection> _logger;

        /// <summary>
        ///     Filter on these keys provided to OptionsStrategy and apply them to the request.
        /// </summary>
        private readonly List<string> _allowedKeys = new()
        {
            Tokens.ArgsEvalTimeout, Tokens.ArgsBatchSize,
            Tokens.RequestId, Tokens.ArgsUserAgent,
            Tokens.ArgMaterializeProperties, Tokens.ArgsBulkResults
        };

        /// <inheritdoc />
        public bool IsSessionBound => _sessionId != null;

        private readonly string? _sessionId;

        /// <summary>
        ///     Initializes a new <see cref="IRemoteConnection" />.
        /// </summary>
        /// <param name="host">The host to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        /// <param name="traversalSource">The name of the traversal source on the server to bind to.</param>
        /// <param name="loggerFactory">A factory to create loggers. If not provided, then nothing will be logged.</param>
        /// <exception cref="ArgumentNullException">Thrown when client is null.</exception>
        public DriverRemoteConnection(string host, int port, string traversalSource = "g",
            ILoggerFactory? loggerFactory = null) : this(
            new GremlinClient(new GremlinServer(host, port), loggerFactory: loggerFactory), traversalSource,
            logger: loggerFactory?.CreateLogger<DriverRemoteConnection>() ?? NullLogger<DriverRemoteConnection>.Instance)
        {
        }

        /// <summary>
        ///     Initializes a new <see cref="IRemoteConnection" />.
        /// </summary>
        /// <param name="client">The <see cref="IGremlinClient" /> that will be used for the connection.</param>
        /// <param name="traversalSource">The name of the traversal source on the server to bind to.</param>
        /// <exception cref="ArgumentNullException">Thrown when client or the traversalSource is null.</exception>
        public DriverRemoteConnection(IGremlinClient client, string traversalSource = "g")
            : this(client, traversalSource, logger: null)
        {
        }

        private DriverRemoteConnection(IGremlinClient client, string traversalSource,
            string? sessionId = null, ILogger<DriverRemoteConnection>? logger = null)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _traversalSource = traversalSource ?? throw new ArgumentNullException(nameof(traversalSource));
            _sessionId = sessionId;

            if (logger == null)
            {
                var loggerFactory = client is GremlinClient gremlinClient
                    ? gremlinClient.LoggerFactory
                    : NullLoggerFactory.Instance;

                logger = loggerFactory.CreateLogger<DriverRemoteConnection>();
            }
            _logger = logger;
        }

        /// <summary>
        ///     Submits <see cref="GremlinLang" /> for evaluation to a remote Gremlin Server.
        /// </summary>
        /// <param name="gremlinLang">The <see cref="GremlinLang" /> to submit.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A <see cref="ITraversal" /> allowing to access the results and side-effects.</returns>
        public async Task<ITraversal<TStart, TEnd>> SubmitAsync<TStart, TEnd>(GremlinLang gremlinLang,
            CancellationToken cancellationToken = default)
        {
            _logger.SubmittingGremlinLang(gremlinLang);
            gremlinLang.AddG(_traversalSource);

            var requestMsg = RequestMessage.Build(gremlinLang.GetGremlin())
                .AddG(_traversalSource)
                .AddBindings(gremlinLang.Parameters);

            foreach (var optionsStrategy in gremlinLang.OptionsStrategies)
            {
                foreach (var pair in optionsStrategy.Configuration)
                {
                    if (_allowedKeys.Contains(pair.Key))
                    {
                        requestMsg.AddField(pair.Key, pair.Value);
                    }
                }
            }

            // Default bulkResults to "true" if not set per-request
            // (consistent with Java RequestOptions.fromGremlinLang and Python extract_request_options)
            if (!requestMsg.HasField(Tokens.ArgsBulkResults))
            {
                requestMsg.AddField(Tokens.ArgsBulkResults, "true");
            }

            var resultSet = await _client.SubmitAsync<Traverser>(requestMsg.Create(), cancellationToken)
                .ConfigureAwait(false);
            return new DriverRemoteTraversal<TStart, TEnd>(resultSet);
        }

        /// <inheritdoc />
        public RemoteTransaction Tx(GraphTraversalSource g)
        {
            var session = new DriverRemoteConnection(_client, _traversalSource, sessionId: Guid.NewGuid().ToString(), logger: _logger);
            return new RemoteTransaction(session, g);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}
