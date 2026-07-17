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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;

namespace Gremlin.Net.Driver.Remote
{
    /// <summary>
    ///     A <see cref="IRemoteConnection"/> that attaches a transactionId to all requests,
    ///     binding them to a server-side transaction.
    /// </summary>
    internal class TransactionRemoteConnection : IRemoteConnection
    {
        private readonly IGremlinClient _client;
        private readonly string _traversalSource;
        private readonly string _transactionId;
        private readonly RemoteTransaction _transaction;
        private bool _closed;

        public TransactionRemoteConnection(IGremlinClient client, string traversalSource, string transactionId, RemoteTransaction transaction)
        {
            _client = client;
            _traversalSource = traversalSource;
            _transactionId = transactionId;
            _transaction = transaction;
        }

        /// <inheritdoc />
        public async Task<ITraversal<TStart, TEnd>> SubmitAsync<TStart, TEnd>(GremlinLang gremlinLang,
            CancellationToken cancellationToken = default)
        {
            if (_closed)
            {
                throw new InvalidOperationException("Transaction is not open");
            }

            var requestMsg = RequestMessage.Build(gremlinLang.GetGremlin())
                .AddG(_traversalSource);

            var parametersString = gremlinLang.GetParametersAsString();
            if (parametersString != "[:]")
            {
                requestMsg.AddParametersString(parametersString);
            }

            foreach (var optionsStrategy in gremlinLang.OptionsStrategies)
            {
                foreach (var pair in optionsStrategy.Configuration)
                {
                    requestMsg.AddField(pair.Key, pair.Value);
                }
            }

            // Default bulkResults to "true" if not set per-request
            if (!requestMsg.HasField(Tokens.ArgsBulkResults))
            {
                requestMsg.AddField(Tokens.ArgsBulkResults, "true");
            }

            // Inject transactionId into the request
            requestMsg.AddField(Tokens.ArgsTransactionId, _transactionId);

            // Route through Transaction's serialized submission to guarantee ordering
            var resultSet = await _transaction.SubmitAsync<Traverser>(requestMsg.Create(), cancellationToken)
                .ConfigureAwait(false);
            return new DriverRemoteTraversal<TStart, TEnd>(resultSet);
        }

        /// <inheritdoc />
        public RemoteTransaction Tx(GraphTraversalSource g)
        {
            // Return the existing transaction. Calling BeginAsync() on it again is idempotent
            // (it is already open) and returns a source bound to the same transaction.
            return _transaction;
        }

        internal void MarkClosed()
        {
            _closed = true;
        }
    }
}
