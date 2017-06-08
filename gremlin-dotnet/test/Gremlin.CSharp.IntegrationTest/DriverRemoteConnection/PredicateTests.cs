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

using Gremlin.CSharp.Process;
using Gremlin.CSharp.Structure;
using Xunit;

namespace Gremlin.CSharp.IntegrationTest.DriverRemoteConnection
{
    public class PredicateTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        [Fact]
        public void ShouldUsePredicatesCombinedWithPAndInHasStep()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);

            var count = g.V().Has("age", P.Gt(30).And(P.Lt(35))).Count().Next();

            Assert.Equal((long) 1, count);
        }

        [Fact]
        public void ShouldUsePWithinInHasStep()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);

            var count = g.V().Has("name", P.Within("josh", "vadas")).Count().Next();

            Assert.Equal((long) 2, count);
        }
    }
}