﻿#region License

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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
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
        public void g_V_Has_Count()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var b = Bindings.Instance;
            var count = g.V().Has("person", "age", b.Of("x", P.Lt(30))).Count().Next();

            Assert.Equal(2, count);
        }

        [Fact]
        public void g_V_Count_Clone()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var t = g.V().Count();

            Assert.Equal(6, t.Next());
            Assert.Equal(6, t.Clone().Next());
            Assert.Equal(6, t.Clone().Next());
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
            var connection = _connectionFactory.CreateRemoteConnection();
            var options = new Dictionary<string,object>
            {
                {"x", "test"},
                {"y", true}
            };
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);
            
            var countWithStrategy = g.WithStrategies(new OptionsStrategy(options)).V().Count().Next();
            Assert.Equal(6, countWithStrategy);
            
            var responseException = Assert.Throws<ResponseException>(() =>
                                 g.With("y").With("x", "test").With(Tokens.ArgsEvalTimeout, 10).Inject(1)
                                  .SideEffect(Lambda.Groovy("Thread.sleep(10000)")).Iterate());
            Assert.Equal(ResponseStatusCode.ServerTimeout, responseException.StatusCode);

        }

        [Fact]
        public void ShouldUseSeedStrategyToReturnDeterministicResults()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection).WithStrategies(new SeedStrategy(664664));

            var shuffledResults = g.V().Values<string>("name").Order().By(Order.Shuffle).ToList();
            Assert.Equal(shuffledResults, g.V().Values<string>("name").Order().By(Order.Shuffle).ToList());
            Assert.Equal(shuffledResults, g.V().Values<string>("name").Order().By(Order.Shuffle).ToList());
            Assert.Equal(shuffledResults, g.V().Values<string>("name").Order().By(Order.Shuffle).ToList());
            Assert.Equal(shuffledResults, g.V().Values<string>("name").Order().By(Order.Shuffle).ToList());
            Assert.Equal(shuffledResults, g.V().Values<string>("name").Order().By(Order.Shuffle).ToList());
        }

        [Fact]
        public async Task ShouldExecuteAsynchronouslyWhenPromiseIsCalled()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var count = await g.V().Count().Promise(t => t.Next());

            Assert.Equal(6, count);
        }

        [Fact]
        public async Task ShouldSupportCancellationForPromise()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            await Assert.ThrowsAsync<TaskCanceledException>(async () =>
                await g.V().Promise(t => t.Iterate(), new CancellationToken(true)));
        }
        
        [Fact]
        public async Task ShouldSupportFurtherTraversalsAfterOneWasCancelled()
        {
            var connection = _connectionFactory.CreateRemoteConnection(connectionPoolSize: 1);
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);
            var cts = new CancellationTokenSource();
            
            var cancelledTask = g.V().Promise(t => t.Iterate(), cts.Token);
            cts.Cancel();
            await Assert.ThrowsAsync<TaskCanceledException>(async () => await cancelledTask);
            
            Assert.True(await g.V().Promise(t => t.HasNext(), CancellationToken.None));
        }
    }
}