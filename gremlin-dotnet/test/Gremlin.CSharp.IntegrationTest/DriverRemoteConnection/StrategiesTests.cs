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

using System.Threading.Tasks;
using Gremlin.CSharp.Process;
using Gremlin.CSharp.Structure;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Gremlin.Net.Process.Traversal.Strategy.Verification;
using Xunit;

namespace Gremlin.CSharp.IntegrationTest.DriverRemoteConnection
{
    public class StrategiesTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        [Fact]
        public void g_V_Count_Next_WithVertexLabelSubgraphStrategy()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g =
                graph.Traversal()
                    .WithRemote(connection)
                    .WithStrategies(new SubgraphStrategy(vertexCriterion: __.HasLabel("person")));

            var count = g.V().Count().Next();

            Assert.Equal((long) 4, count);
        }

        [Fact]
        public void g_E_Count_Next_WithVertexAndEdgeLabelSubgraphStrategy()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g =
                graph.Traversal()
                    .WithRemote(connection)
                    .WithStrategies(new SubgraphStrategy(vertexCriterion: __.HasLabel("person"),
                        edgeCriterion: __.HasLabel("created")));

            var count = g.E().Count().Next();

            Assert.Equal((long)0, count);
        }

        [Fact]
        public void g_V_Label_Dedup_Count_Next_WithVertexLabelSubgraphStrategy()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g =
                graph.Traversal()
                    .WithRemote(connection)
                    .WithStrategies(new SubgraphStrategy(vertexCriterion: __.HasLabel("person")));

            var count = g.V().Label().Dedup().Count().Next();

            Assert.Equal((long)1, count);
        }

        [Fact]
        public void g_V_Label_Dedup_Next_WWithVertexLabelSubgraphStrategy()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g =
                graph.Traversal()
                    .WithRemote(connection)
                    .WithStrategies(new SubgraphStrategy(vertexCriterion: __.HasLabel("person")));

            var label = g.V().Label().Dedup().Next();

            Assert.Equal("person", label);
        }

        [Fact]
        public void g_V_Count_Next_WithVertexHasPropertySubgraphStrategy()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g =
                graph.Traversal()
                    .WithRemote(connection)
                    .WithStrategies(new SubgraphStrategy(vertexCriterion: __.Has("name", "marko")));

            var count = g.V().Count().Next();

            Assert.Equal((long)1, count);
        }

        [Fact]
        public void g_E_Count_Next_WithEdgeLimitSubgraphStrategy()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g =
                graph.Traversal()
                    .WithRemote(connection)
                    .WithStrategies(new SubgraphStrategy(edgeCriterion: __.Limit(0)));

            var count = g.E().Count().Next();

            Assert.Equal((long)0, count);
        }

        [Fact]
        public void g_V_Label_Dedup_Next_WithVertexHasPropertySubgraphStrategy()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g =
                graph.Traversal()
                    .WithRemote(connection)
                    .WithStrategies(new SubgraphStrategy(vertexCriterion: __.Has("name", "marko")));

            var label = g.V().Label().Dedup().Next();

            Assert.Equal("person", label);
        }

        [Fact]
        public void g_V_ValuesXnameX_Next_WithVertexHasPropertySubgraphStrategy()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g =
                graph.Traversal()
                    .WithRemote(connection)
                    .WithStrategies(new SubgraphStrategy(vertexCriterion: __.Has("name", "marko")));

            var name = g.V().Values("name").Next();

            Assert.Equal("marko", name);
        }

        [Fact]
        public void g_V_Count_Next_WithComputer()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection).WithComputer();

            var count = g.V().Count().Next();

            Assert.Equal((long)6, count);
        }

        [Fact]
        public void g_E_Count_Next_WithComputer()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection).WithComputer();

            var count = g.E().Count().Next();

            Assert.Equal((long)6, count);
        }

        [Fact]
        public async Task ShouldThrowWhenModifyingTraversalSourceWithReadOnlyStrategy()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection).WithStrategies(new ReadOnlyStrategy());

            await Assert.ThrowsAsync<ResponseException>(async () => await g.AddV("person").Promise(t => t.Next()));
        }
    }
}