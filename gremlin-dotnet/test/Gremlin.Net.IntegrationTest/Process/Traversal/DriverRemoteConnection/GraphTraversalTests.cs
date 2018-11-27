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

using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Xunit;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection
{
    public class GraphTraversalTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        [Fact]
        public void g_V_Count()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var count = g.V().Count().Next();

            Assert.Equal(6, count);
        }

        [Fact]
        public void g_VX1X_Next()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var vertex = g.V(1).Next();

            Assert.Equal(new Vertex(1), vertex);
            Assert.Equal(1, vertex.Id);
        }

        [Fact]
        public void g_VX1X_NextTraverser()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var traverser = g.V(1).NextTraverser();

            Assert.Equal(new Traverser(new Vertex(1)), traverser);
        }

        [Fact]
        public void g_VX1X_ToList()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var list = g.V(1).ToList();

            Assert.Equal(1, list.Count);
        }

        [Fact]
        public void g_V_RepeatXBothX_TimesX5X_NextX10X()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var result = g.V().Repeat(__.Both()).Times(5).Next(10);

            Assert.Equal(10, result.Count());
        }

        [Fact]
        public void g_V_RepeatXOutX_TimesX2X_ValuesXNameX()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var t = g.V().Repeat(__.Out()).Times(2).Values<string>("name");
            var names = t.ToList();

            Assert.Equal((long) 2, names.Count);
            Assert.Contains("lop", names);
            Assert.Contains("ripple", names);
        }

        [Fact]
        public void ShortestPathTest()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var shortestPath =
                g.V(5).Repeat(__.Both().SimplePath()).Until(__.HasId(6)).Limit<Vertex>(1).Path().Next();

            Assert.Equal(4, shortestPath.Count);
            Assert.Equal(new Vertex(6), shortestPath[3]);
        }

        [Fact]
        public void ValueMapTest()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var result = g.V(1).ValueMap<string, IList<object>>().Next();
            Assert.Equal(
                new Dictionary<string, IList<object>>
                {
                    { "age", new List<object> { 29 } },
                    { "name", new List<object> { "marko" } }
                },
                result);
        }

        [Fact]
        public void ValueMapWithListConversionTest()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var result = g.V(1).ValueMap<string, IList<int>>("age").Next();
            Assert.Equal(new Dictionary<string, IList<int>>
            {
                { "age", new List<int> { 29 } }
            }, result);
        }

        [Fact]
        public void GroupedEdgePropertyConversionTest()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);
            var result = g.V().HasLabel("software").Group<string, double>().By("name")
                          .By(__.BothE().Values<double>("weight").Mean<double>()).Next();
            Assert.Equal(new Dictionary<string, double>
            {
                { "ripple", 1D },
                { "lop", 1d/3d }
            }, result);
        }

        [Fact]
        public void ShouldUseBindingsInTraversal()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var b = new Bindings();
            var count = g.V().Has(b.Of("propertyKey", "name"), b.Of("propertyValue", "marko")).OutE().Count().Next();

            Assert.Equal(3, count);
        }  

        [Fact]
        public void ShouldUseOptionsInTraversal()
        {
            // smoke test to validate serialization of OptionsStrategy. no way to really validate this from an integration
            // test perspective because there's no way to access the internals of the strategy via bytecode
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var options = new Dictionary<string,object>
            {
                {"x", "test"},
                {"y", true}
            };
            var g = graph.Traversal().WithRemote(connection);

            var b = new Bindings();
            var countWithStrategy = g.WithStrategies(new OptionsStrategy(options)).V().Count().Next();
            Assert.Equal(6, countWithStrategy);

            var countWith = g.With("x", "test").With("y", true).V().Count().Next();
            Assert.Equal(6, countWith);
        }

        [Fact]
        public async Task ShouldExecuteAsynchronouslyWhenPromiseIsCalled()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var count = await g.V().Count().Promise(t => t.Next());

            Assert.Equal(6, count);
        }
    }
}
