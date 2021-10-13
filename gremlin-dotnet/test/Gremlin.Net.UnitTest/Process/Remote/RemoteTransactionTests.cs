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
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
using Moq;
using Xunit;

namespace Gremlin.Net.UnitTest.Process.Remote
{
    public class RemoteTransactionTests
    {
        [Fact]
        public void ShouldNotAllowBeginMoreThanOnce()
        {
            var g = AnonymousTraversalSource.Traversal()
                .WithRemote(new DriverRemoteConnection(Mock.Of<IGremlinClient>()));
            var tx = g.Tx();
            tx.Begin();

            Assert.Throws<InvalidOperationException>(() => tx.Begin());
        }
        
        [Fact]
        public void ShouldNotSupportChildTransactions()
        {
            var g = AnonymousTraversalSource.Traversal()
                .WithRemote(new DriverRemoteConnection(Mock.Of<IGremlinClient>()));
            var tx = g.Tx();
            
            var gtx = tx.Begin();
            Assert.Throws<InvalidOperationException>(() => gtx.Tx());
        }
    }
}