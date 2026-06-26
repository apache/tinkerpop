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
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.IntegrationTest.Util;
using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class TransactionTests
    {
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"]!;
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);

        private GremlinClient CreateClient()
        {
            return new GremlinClient(new GremlinServer(TestHost, TestPort));
        }

        private async Task<long> GetCount(GremlinClient client, string label)
        {
            var msg = RequestMessage.Build($"g.V().hasLabel('{label}').count()")
                .AddG("gtx").Create();
            var resultSet = await client.SubmitAsync<long>(msg);
            var enumerator = resultSet.GetAsyncEnumerator();
            await enumerator.MoveNextAsync();
            return enumerator.Current;
        }

        /// <summary>
        ///     Drops all vertices from the transactional graph to prevent cross-test contamination.
        /// </summary>
        private async Task DropGraph(GremlinClient client)
        {
            var msg = RequestMessage.Build("g.V().drop()").AddG("gtx").Create();
            await client.SubmitAsync<object>(msg);
        }

        [Fact]
        public async Task ShouldCommitTransaction()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx = client.Transact("gtx");
            await tx.BeginAsync();
            Assert.True(tx.IsOpen);

            await tx.SubmitAsync<object>("g.addV('person').property('name','alice')");

            // Uncommitted data not visible outside the transaction
            Assert.Equal(0L, await GetCount(client, "person"));

            await tx.CommitAsync();
            Assert.False(tx.IsOpen);

            // Committed data visible
            Assert.Equal(1L, await GetCount(client, "person"));
        }

        [Fact]
        public async Task ShouldRollbackTransaction()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx = client.Transact("gtx");
            await tx.BeginAsync();

            await tx.SubmitAsync<object>("g.addV('person').property('name','bob')");

            await tx.RollbackAsync();
            Assert.False(tx.IsOpen);

            Assert.Equal(0L, await GetCount(client, "person"));
        }

        [Fact]
        public async Task ShouldSupportIntraTransactionConsistency()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx = client.Transact("gtx");
            await tx.BeginAsync();

            await tx.SubmitAsync<object>("g.addV('test').property('name','A')");

            // Read-your-own-writes
            var resultSet = await tx.SubmitAsync<long>("g.V().hasLabel('test').count()");
            var enumerator = resultSet.GetAsyncEnumerator();
            await enumerator.MoveNextAsync();
            Assert.Equal(1L, enumerator.Current);

            await tx.SubmitAsync<object>("g.addV('test').property('name','B')");
            await tx.CommitAsync();

            Assert.Equal(2L, await GetCount(client, "test"));
        }

        [Fact]
        public async Task ShouldThrowOnSubmitAfterCommit()
        {
            using var client = CreateClient();
            var tx = client.Transact("gtx");
            await tx.BeginAsync();
            await tx.SubmitAsync<object>("g.addV()");
            await tx.CommitAsync();

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => tx.SubmitAsync<object>("g.V().count()"));
        }

        [Fact]
        public async Task ShouldThrowOnSubmitAfterRollback()
        {
            using var client = CreateClient();
            var tx = client.Transact("gtx");
            await tx.BeginAsync();
            await tx.SubmitAsync<object>("g.addV()");
            await tx.RollbackAsync();

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => tx.SubmitAsync<object>("g.V().count()"));
        }

        [Fact]
        public async Task ShouldThrowOnDoubleBegin()
        {
            using var client = CreateClient();
            var tx = client.Transact("gtx");
            await tx.BeginAsync();

            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.BeginAsync());
        }

        [Fact]
        public async Task ShouldThrowOnCommitWhenNotOpen()
        {
            using var client = CreateClient();
            var tx = client.Transact("gtx");

            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.CommitAsync());
        }

        [Fact]
        public async Task ShouldThrowOnRollbackWhenNotOpen()
        {
            using var client = CreateClient();
            var tx = client.Transact("gtx");

            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.RollbackAsync());
        }

        [Fact]
        public async Task ShouldReturnNullTransactionIdBeforeBegin()
        {
            using var client = CreateClient();
            var tx = client.Transact("gtx");

            Assert.Null(tx.TransactionId);

            await tx.BeginAsync();
            Assert.NotNull(tx.TransactionId);
            Assert.True(tx.TransactionId!.Length > 0);
        }

        [Fact]
        public async Task ShouldRollbackOnDisposeByDefault()
        {
            using var client = CreateClient();
            await DropGraph(client);

            await using (var tx = client.Transact("gtx"))
            {
                await tx.BeginAsync();
                await tx.SubmitAsync<object>("g.addV('person').property('name','dispose_test')");
                // No commit - dispose should rollback
            }

            Assert.Equal(0L, await GetCount(client, "person"));
        }

        [Fact]
        public async Task ShouldIsolateConcurrentTransactions()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx1 = client.Transact("gtx");
            await tx1.BeginAsync();
            var tx2 = client.Transact("gtx");
            await tx2.BeginAsync();

            await tx1.SubmitAsync<object>("g.addV('tx1')");
            await tx2.SubmitAsync<object>("g.addV('tx2')");

            // tx1 should not see tx2's data
            var rs = await tx1.SubmitAsync<long>("g.V().hasLabel('tx2').count()");
            var e = rs.GetAsyncEnumerator();
            await e.MoveNextAsync();
            Assert.Equal(0L, e.Current);

            await tx1.CommitAsync();
            await tx2.CommitAsync();

            Assert.Equal(1L, await GetCount(client, "tx1"));
            Assert.Equal(1L, await GetCount(client, "tx2"));
        }

        [Fact]
        public async Task ShouldOpenAndCloseManyTransactionsSequentially()
        {
            using var client = CreateClient();
            await DropGraph(client);
            const int numTransactions = 50;

            for (int i = 0; i < numTransactions; i++)
            {
                var tx = client.Transact("gtx");
                await tx.BeginAsync();
                await tx.SubmitAsync<object>("g.addV('stress')");
                await tx.CommitAsync();
            }

            Assert.Equal((long)numTransactions, await GetCount(client, "stress"));
        }

        [Fact]
        public async Task ShouldKeepTransactionOpenAfterTraversalError()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx = client.Transact("gtx");
            await tx.BeginAsync();

            await tx.SubmitAsync<object>("g.addV('good_vertex')");

            try
            {
                await tx.SubmitAsync<object>("g.V().fail()");
            }
            catch
            {
                // expected
            }

            Assert.True(tx.IsOpen);
            await tx.RollbackAsync();

            Assert.Equal(0L, await GetCount(client, "good_vertex"));
        }

        [Fact]
        public async Task ShouldWorkWithTraversalApi()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var connection = new DriverRemoteConnection(client, "gtx");

            var g = AnonymousTraversalSource.Traversal().With(connection);

            var tx = g.Tx();
            var gtx = await tx.BeginAsync();
            await gtx.AddV("val").Promise(t => t.Iterate());
            await tx.CommitAsync();

            var count = await g.V().HasLabel("val").Count().Promise(t => t.Next());
            Assert.Equal(1L, count);
        }

        [Fact]
        public async Task ShouldRejectBeginOnNonTransactionalGraph()
        {
            using var client = CreateClient();
            var tx = client.Transact("gclassic");

            var ex = await Assert.ThrowsAsync<ResponseException>(() => tx.BeginAsync());
            Assert.Contains("Graph does not support transactions", ex.Message);
        }

        [Fact]
        public async Task ShouldReturnSameTransactionFromGtxTx()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var connection = new DriverRemoteConnection(client, "gtx");
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var tx = g.Tx();
            var gtx = await tx.BeginAsync();
            var sameTx = gtx.Tx();

            Assert.Same(tx, sameTx);

            await sameTx.RollbackAsync();
        }

        [Fact]
        public async Task ShouldThrowOnBeginFromGtxTx()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var connection = new DriverRemoteConnection(client, "gtx");
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var tx = g.Tx();
            var gtx = await tx.BeginAsync();
            var sameTx = gtx.Tx();

            await Assert.ThrowsAsync<InvalidOperationException>(() => sameTx.BeginAsync());

            await tx.RollbackAsync();
        }

        [Fact]
        public async Task ShouldThrowOnDoubleCommit()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx = client.Transact("gtx");
            await tx.BeginAsync();
            await tx.CommitAsync();

            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.CommitAsync());
        }

        [Fact]
        public async Task ShouldThrowOnDoubleRollback()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx = client.Transact("gtx");
            await tx.BeginAsync();
            await tx.RollbackAsync();

            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.RollbackAsync());
        }

        [Fact]
        public async Task ShouldNotAllowBeginAfterCommit()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx = client.Transact("gtx");
            await tx.BeginAsync();
            await tx.CommitAsync();

            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.BeginAsync());
        }

        [Fact]
        public async Task ShouldNotAllowBeginAfterRollback()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx = client.Transact("gtx");
            await tx.BeginAsync();
            await tx.RollbackAsync();

            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.BeginAsync());
        }

        [Fact]
        public async Task ShouldIsolateTransactionalAndNonTransactionalRequests()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx = client.Transact("gtx");
            await tx.BeginAsync();

            await tx.SubmitAsync<object>("g.addV('tx_data')");

            // Non-transactional read should not see uncommitted data
            Assert.Equal(0L, await GetCount(client, "tx_data"));

            await tx.CommitAsync();

            Assert.Equal(1L, await GetCount(client, "tx_data"));
        }

        [Fact]
        public async Task ShouldCommitViaGtxTx()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var connection = new DriverRemoteConnection(client, "gtx");
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var tx = g.Tx();
            var gtx = await tx.BeginAsync();
            await gtx.AddV("gtx_commit_test").Promise(t => t.Iterate());
            await gtx.Tx().CommitAsync();

            Assert.Equal(1L, await GetCount(client, "gtx_commit_test"));
        }

        [Fact]
        public async Task ShouldCleanUpOnBeginFailure()
        {
            using var client = CreateClient();
            var tx = client.Transact("gclassic");

            try
            {
                await tx.BeginAsync();
            }
            catch
            {
                // expected
            }

            Assert.False(tx.IsOpen);
            Assert.Null(tx.TransactionId);

            // Cannot begin again
            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.BeginAsync());
        }
        [Fact]
        public async Task ShouldMultiRollbackTransactions()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx1 = client.Transact("gtx");
            await tx1.BeginAsync();
            var tx2 = client.Transact("gtx");
            await tx2.BeginAsync();

            await tx1.SubmitAsync<object>("g.addV('multi_rb1')");
            await tx2.SubmitAsync<object>("g.addV('multi_rb2')");

            await tx1.RollbackAsync();
            Assert.Equal(0L, await GetCount(client, "multi_rb1"));

            await tx2.RollbackAsync();
            Assert.Equal(0L, await GetCount(client, "multi_rb2"));
        }

        [Fact]
        public async Task ShouldMultiCommitAndRollback()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx1 = client.Transact("gtx");
            await tx1.BeginAsync();
            var tx2 = client.Transact("gtx");
            await tx2.BeginAsync();

            await tx1.SubmitAsync<object>("g.addV('multi_cr1')");
            await tx2.SubmitAsync<object>("g.addV('multi_cr2')");

            await tx1.CommitAsync();
            Assert.Equal(1L, await GetCount(client, "multi_cr1"));

            await tx2.RollbackAsync();
            Assert.Equal(0L, await GetCount(client, "multi_cr2"));
        }

        [Fact]
        public async Task ShouldRollbackOnClientClose()
        {
            var client1 = CreateClient();
            await DropGraph(client1);
            var tx = client1.Transact("gtx");
            await tx.BeginAsync();
            await tx.SubmitAsync<object>("g.addV('client_close_test')");

            client1.Dispose();

            Assert.False(tx.IsOpen);

            using var client2 = CreateClient();
            Assert.Equal(0L, await GetCount(client2, "client_close_test"));
        }

        [Fact]
        public async Task ShouldRollbackOnDrcClose()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var connection = new DriverRemoteConnection(client, "gtx");
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var tx = g.Tx();
            var gtx = await tx.BeginAsync();
            await gtx.AddV("drc_close_test").Promise(t => t.Iterate());

            connection.Dispose();

            Assert.False(tx.IsOpen);

            using var client2 = CreateClient();
            Assert.Equal(0L, await GetCount(client2, "drc_close_test"));
        }

        // Sentinel exception used by the body-throws closure test to assert the exact
        // original error (type + message) propagates out of the closure wrapper.
        private sealed class SentinelTransactionException : Exception
        {
            public SentinelTransactionException(string message) : base(message)
            {
            }
        }

        [Fact]
        public async Task ShouldCommitOnSuccessWithExecuteInTxAsync()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var connection = new DriverRemoteConnection(client, "gtx");
            var g = AnonymousTraversalSource.Traversal().With(connection);

            await g.ExecuteInTxAsync(async gtx => await gtx.AddV("person").Promise(t => t.Iterate()));

            // The closure committed automatically on success, so the vertex is persisted.
            Assert.Equal(1L, await GetCount(client, "person"));
        }

        [Fact]
        public async Task ShouldRollbackAndRethrowWhenExecuteInTxAsyncBodyThrows()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var connection = new DriverRemoteConnection(client, "gtx");
            var g = AnonymousTraversalSource.Traversal().With(connection);

            const string sentinelMessage = "sentinel-body-error-3f1c8e";

            // (i) The exact original exception (type + message) propagates to the caller.
            var ex = await Assert.ThrowsAsync<SentinelTransactionException>(() =>
                g.ExecuteInTxAsync(async gtx =>
                {
                    await gtx.AddV("person").Promise(t => t.Iterate());
                    throw new SentinelTransactionException(sentinelMessage);
                }));
            Assert.Equal(sentinelMessage, ex.Message);

            // (ii) The closure rolled back automatically, so the vertex is NOT persisted.
            Assert.Equal(0L, await GetCount(client, "person"));
        }

        [Fact]
        public async Task ShouldReturnBodyValueFromEvaluateInTxAsync()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var connection = new DriverRemoteConnection(client, "gtx");
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var n = await g.EvaluateInTxAsync(async gtx =>
            {
                await gtx.AddV("person").Promise(t => t.Iterate());
                await gtx.AddV("person").Promise(t => t.Iterate());
                return await gtx.V().Count().Promise(t => t.Next());
            });

            // The body counted the two vertices it added within the transaction.
            Assert.Equal(2L, n);

            // The closure committed, so the same count is visible afterwards.
            Assert.Equal(2L, await GetCount(client, "person"));
        }

        [Fact]
        public async Task ShouldThrowWhenOpeningNestedTransactionInsideExecuteInTxAsync()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var connection = new DriverRemoteConnection(client, "gtx");
            var g = AnonymousTraversalSource.Traversal().With(connection);

            // Opening a SECOND transaction from inside the body must throw. The closure body's
            // own commit will then fail because the body threw, surfacing the nesting error.
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                g.ExecuteInTxAsync(async gtx =>
                {
                    // gtx.Tx() legitimately returns the SAME transaction (it must not throw);
                    // calling BeginAsync() on it opens a second transaction and must throw.
                    await gtx.Tx().BeginAsync();
                }));
        }

        [Fact]
        public async Task ShouldPropagateCommitErrorFromExecuteInTxAsync()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var connection = new DriverRemoteConnection(client, "gtx");
            var g = AnonymousTraversalSource.Traversal().With(connection);

            // Drive a commit failure without a mock: from inside the body, terminate the
            // server-side transaction out-of-band (rollback by its transactionId via a second
            // client). The wrapper's automatic CommitAsync then fails server-side with
            // "Transaction not found", and that commit error must propagate out of ExecuteInTxAsync.
            using var sideClient = CreateClient();

            var ex = await Assert.ThrowsAsync<ResponseException>(() =>
                g.ExecuteInTxAsync(async gtx =>
                {
                    await gtx.AddV("person").Promise(t => t.Iterate());

                    // Roll back this very transaction out-of-band so the upcoming commit fails.
                    var txId = gtx.Tx().TransactionId!;
                    var rollbackMsg = RequestMessage.Build("g.tx().rollback()")
                        .AddG("gtx")
                        .AddField(Tokens.ArgsTransactionId, txId)
                        .Create();
                    await sideClient.SubmitAsync<object>(rollbackMsg);
                }));

            Assert.Contains("Transaction not found", ex.Message);

            // The out-of-band rollback already discarded the work, so nothing is persisted.
            Assert.Equal(0L, await GetCount(client, "person"));
        }

        [Fact]
        public async Task ShouldSerializeUnawaitedSubmissions()
        {
            using var client = CreateClient();
            await DropGraph(client);
            var tx = client.Transact("gtx");
            await tx.BeginAsync();

            // Fire submissions from the same thread without awaiting between them.
            // The semaphore serializes them so the server processes them in order.
            // Each step depends on the prior one completing first.
            var t1 = tx.SubmitAsync<object>("g.addV('chain').property('pos', 0)");
            var t2 = tx.SubmitAsync<object>("g.V().has('chain','pos',0).property('pos', 1)");
            var t3 = tx.SubmitAsync<object>("g.V().has('chain','pos',1).property('pos', 2)");

            await Task.WhenAll(t1, t2, t3);

            // Verify the chain completed: final vertex should have pos=2
            var rs = await tx.SubmitAsync<long>("g.V().has('chain','pos',2).count()");
            var enumerator = rs.GetAsyncEnumerator();
            await enumerator.MoveNextAsync();
            Assert.Equal(1L, enumerator.Current);

            await tx.CommitAsync();
        }
    }
}
