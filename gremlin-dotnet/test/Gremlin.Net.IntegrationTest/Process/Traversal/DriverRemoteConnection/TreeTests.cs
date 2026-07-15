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

using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection
{
    [Collection("GremlinServerTests")]
    public class TreeTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        [Fact]
        public void g_VX1X_Out_Out_Tree_ByName()
        {
            var g = AnonymousTraversalSource.Traversal().With(_connectionFactory.CreateRemoteConnection());

            var tree = (Tree) g.V(1).Out().Out().Tree<object>().By("name").Next()!;

            // marko is the single root
            var rootNodes = tree.RootNodes();
            Assert.Single(rootNodes);
            Assert.Contains("marko", rootNodes);

            // marko -> josh -> {lop, ripple} : marko, josh, lop, ripple
            Assert.Equal(4, tree.NodeCount());

            // depth 0 is the root
            Assert.Equal(new object?[] { "marko" }, tree.GetNodesAtDepth(0));

            // depth 2 holds the software vertices reachable via two out() steps
            var depthTwo = tree.GetNodesAtDepth(2);
            Assert.Contains("lop", depthTwo);
            Assert.Contains("ripple", depthTwo);

            // leaves are the software vertices
            var leaves = tree.GetLeafNodes();
            Assert.Contains("lop", leaves);
            Assert.Contains("ripple", leaves);

            Assert.False(tree.IsLeaf());

            // marko -> josh -> {lop, ripple}; lop/ripple are siblings with unspecified order
            const string optionA = "|--marko\n   |--josh\n      |--lop\n      |--ripple";
            const string optionB = "|--marko\n   |--josh\n      |--ripple\n      |--lop";
            var pretty = tree.PrettyPrint();
            Assert.True(pretty == optionA || pretty == optionB, $"Unexpected prettyPrint output:\n{pretty}");
        }

        [Fact]
        public void g_VX1X_Out_Out_Tree_Vertices()
        {
            var g = AnonymousTraversalSource.Traversal().With(_connectionFactory.CreateRemoteConnection());

            var tree = (Tree) g.V(1).Out().Out().Tree<object>().Next()!;

            Assert.Equal(4, tree.NodeCount());
            Assert.Single(tree.RootNodes());
        }
    }
}
