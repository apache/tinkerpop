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
using System.Threading.Tasks;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection
{
    public class GraphTraversalTransactionTests : IDisposable
    {
        private readonly IRemoteConnection _connection = new RemoteConnectionFactory().CreateRemoteConnection("gtx");

        [IgnoreIfTransactionsNotSupportedFact]
        public async Task ShouldSupportRemoteTransactionsCommit()
        {
            var g = AnonymousTraversalSource.Traversal().WithRemote(_connection);
            var tx = g.Tx();
            var gtx = tx.Begin();
            await gtx.AddV("person").Property("name", "jorge").Promise(t => t.Iterate()).ConfigureAwait(false);
            await gtx.AddV("person").Property("name", "josh").Promise(t => t.Iterate()).ConfigureAwait(false);
            
            // Assert within the transaction
            var count = await gtx.V().Count().Promise(t => t.Next()).ConfigureAwait(false);
            Assert.Equal(2, count);
            
            // Vertices should not be visible in a different transaction before commiting
            count = await g.V().Count().Promise(t => t.Next()).ConfigureAwait(false);
            Assert.Equal(0, count);
            
            // Now commit changes to test outside of the transaction
            await tx.CommitAsync().ConfigureAwait(false);

            count = await g.V().Count().Promise(t => t.Next()).ConfigureAwait(false);
            Assert.Equal(2, count);
        }
        
        [IgnoreIfTransactionsNotSupportedFact]
        public async Task ShouldSupportRemoteTransactionsRollback()
        {
            var g = AnonymousTraversalSource.Traversal().WithRemote(_connection);
            var tx = g.Tx();
            var gtx = tx.Begin();
            await gtx.AddV("person").Property("name", "jorge").Promise(t => t.Iterate()).ConfigureAwait(false);
            await gtx.AddV("person").Property("name", "josh").Promise(t => t.Iterate()).ConfigureAwait(false);
            
            // Assert within the transaction
            var count = await gtx.V().Count().Promise(t => t.Next()).ConfigureAwait(false);
            Assert.Equal(2, count);
            
            // Now rollback changes to test outside of the transaction
            await tx.RollbackAsync().ConfigureAwait(false);

            count = await g.V().Count().Promise(t => t.Next()).ConfigureAwait(false);
            Assert.Equal(0, count);
            
            g.V().Count().Next();
        }

        public void Dispose()
        {
            EmptyGraph();
        }

        private void EmptyGraph()
        {
            var g = AnonymousTraversalSource.Traversal().WithRemote(_connection);
            g.V().Drop().Iterate();
        }
    }

    public sealed class IgnoreIfTransactionsNotSupportedFact : FactAttribute
    {
        public IgnoreIfTransactionsNotSupportedFact()
        {
            if (!TransactionsSupported)
            {
                Skip = "Transactions not supported";
            }
        }

        private static bool TransactionsSupported =>
            Convert.ToBoolean(Environment.GetEnvironmentVariable("TEST_TRANSACTIONS"));
    }
}