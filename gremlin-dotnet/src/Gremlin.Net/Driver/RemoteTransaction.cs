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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Controls an explicit remote transaction. Created via
    ///     <c>GremlinClient.Transact()</c> or <c>g.Tx()</c>.
    ///     The transaction is not started until <see cref="BeginAsync"/> is called.
    ///
    ///     All submissions are serialized internally via a semaphore to guarantee
    ///     the server receives requests in order, even if the caller does not await
    ///     each call before issuing the next.
    ///
    ///     This class is NOT thread-safe. Do not share a RemoteTransaction across
    ///     multiple threads without external synchronization.
    /// </summary>
    public class RemoteTransaction : IGremlinClient, IAsyncDisposable
    {
        private readonly IGremlinClient _client;
        private readonly string _traversalSource;
        // Serializes all submissions to guarantee ordering.
        private readonly SemaphoreSlim _submitLock = new(1, 1);
        private string? _transactionId;
        private bool _isOpen;
        private bool _failed;
        private TransactionRemoteConnection? _txConnection;

        internal RemoteTransaction(IGremlinClient client, string traversalSource)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _traversalSource = traversalSource ?? throw new ArgumentNullException(nameof(traversalSource));
        }

        /// <summary>
        ///     Gets the server-generated transaction ID, or null if the transaction has not yet been started.
        /// </summary>
        public string? TransactionId => _transactionId;

        /// <summary>
        ///     Gets whether the transaction is currently open.
        /// </summary>
        public bool IsOpen => _isOpen;

        /// <summary>
        ///     Starts the transaction and returns a transaction-bound <see cref="GraphTraversalSource"/>.
        ///     <para>
        ///     This method is idempotent: calling it while a transaction is already open does not send a second
        ///     begin to the server and does not throw - it reuses the existing transaction ID and returns a source
        ///     bound to the same transaction. A transaction is single-use, so calling it after the transaction has
        ///     been closed (commit/rollback/failed begin) throws.
        ///     </para>
        /// </summary>
        /// <param name="cancellationToken">The token to cancel the operation.</param>
        /// <returns>A <see cref="GraphTraversalSource"/> bound to this transaction.</returns>
        /// <exception cref="InvalidOperationException">Thrown if the transaction has already been closed.</exception>
        public async Task<GraphTraversalSource> BeginAsync(CancellationToken cancellationToken = default)
        {
            if (_failed)
            {
                throw new InvalidOperationException(
                    "Transaction is closed and cannot be reused; begin a new transaction");
            }

            // idempotent: if a transaction is already open, reuse the existing transactionId without sending a
            // second begin to the server, and return a source bound to the same transaction
            if (!_isOpen)
            {
                var requestMsg = RequestMessage.Build("g.tx().begin()")
                    .AddG(_traversalSource)
                    .Create();

                await _submitLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    List<object> results;
                    try
                    {
                        var resultSet = await _client.SubmitAsync<object>(requestMsg, cancellationToken)
                            .ConfigureAwait(false);
                        results = await resultSet.ToListAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch
                    {
                        _failed = true;
                        throw;
                    }

                    if (results.Count == 0)
                    {
                        _failed = true;
                        throw new InvalidOperationException("Server did not return transaction ID");
                    }

                    if (results[0] is Dictionary<object, object> resultMap &&
                        resultMap.TryGetValue("transactionId", out var txIdObj) &&
                        txIdObj is string txId && !string.IsNullOrEmpty(txId))
                    {
                        _transactionId = txId;
                    }
                    else
                    {
                        _failed = true;
                        throw new InvalidOperationException("Server did not return transaction ID in expected format");
                    }

                    // assign _txConnection before publishing _isOpen=true so any thread that observes the
                    // transaction as open is guaranteed to also see a non-null _txConnection
                    _txConnection = new TransactionRemoteConnection(_client, _traversalSource, _transactionId, this);
                    _isOpen = true;
                    (_client as GremlinClient)?.TrackTransaction(this);
                }
                finally
                {
                    _submitLock.Release();
                }
            }

            return new GraphTraversalSource(
                new List<ITraversalStrategy>(),
                new GremlinLang(),
                _txConnection!);
        }

        /// <summary>
        ///     Commits the transaction.
        /// </summary>
        /// <param name="cancellationToken">The token to cancel the operation.</param>
        /// <exception cref="InvalidOperationException">Thrown if the transaction is not open.</exception>
        public async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            await CloseTransactionAsync("g.tx().commit()", cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Rolls back the transaction.
        /// </summary>
        /// <param name="cancellationToken">The token to cancel the operation.</param>
        /// <exception cref="InvalidOperationException">Thrown if the transaction is not open.</exception>
        public async Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            await CloseTransactionAsync("g.tx().rollback()", cancellationToken).ConfigureAwait(false);
        }

        private async Task CloseTransactionAsync(string script, CancellationToken cancellationToken)
        {
            if (!_isOpen)
            {
                throw new InvalidOperationException("Transaction is not open");
            }

            var requestMsg = RequestMessage.Build(script)
                .AddG(_traversalSource)
                .AddField(Tokens.ArgsTransactionId, _transactionId!)
                .Create();

            await _submitLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var resultSet = await _client.SubmitAsync<object>(requestMsg, cancellationToken).ConfigureAwait(false);
                // Drain the result to surface any GraphBinary-level errors from the response body
                await resultSet.ToListAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _submitLock.Release();
            }
            _isOpen = false;
            _failed = true; // Terminal state: transaction cannot be reused
            _txConnection?.MarkClosed();
            (_client as GremlinClient)?.UntrackTransaction(this);
        }

        /// <summary>
        ///     Submits a <see cref="RequestMessage"/> within this transaction.
        ///     Submissions are serialized to guarantee the server receives them in order.
        /// </summary>
        public async Task<ResultSet<T>> SubmitAsync<T>(RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            if (!_isOpen)
            {
                throw new InvalidOperationException("Transaction is not open");
            }

            await _submitLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await _client.SubmitAsync<T>(requestMessage, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _submitLock.Release();
            }
        }

        /// <summary>
        ///     Submits a plain gremlin-lang string within this transaction.
        ///     The transactionId is automatically attached.
        /// </summary>
        /// <param name="gremlin">The Gremlin query string.</param>
        /// <param name="cancellationToken">The token to cancel the operation.</param>
        /// <returns>A <see cref="ResultSet{T}"/> containing the results.</returns>
        /// <exception cref="InvalidOperationException">Thrown if the transaction is not open.</exception>
        public async Task<ResultSet<T>> SubmitAsync<T>(string gremlin, CancellationToken cancellationToken = default)
        {
            if (!_isOpen)
            {
                throw new InvalidOperationException("Transaction is not open");
            }

            var requestMsg = RequestMessage.Build(gremlin)
                .AddG(_traversalSource)
                .AddField(Tokens.ArgsTransactionId, _transactionId!)
                .Create();

            await _submitLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await _client.SubmitAsync<T>(requestMsg, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _submitLock.Release();
            }
        }

        /// <summary>
        ///     Disposes the transaction asynchronously. Default behavior is rollback.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_isOpen)
            {
                try
                {
                    await RollbackAsync().ConfigureAwait(false);
                }
                catch
                {
                    _isOpen = false;
                    _failed = true;
                    _txConnection?.MarkClosed();
                }
            }
        }

        /// <summary>
        ///     Synchronous dispose (required by IGremlinClient/IDisposable).
        ///     Does not attempt rollback. Use <c>await using</c> for proper cleanup.
        /// </summary>
        public void Dispose()
        {
        }
    }
}
