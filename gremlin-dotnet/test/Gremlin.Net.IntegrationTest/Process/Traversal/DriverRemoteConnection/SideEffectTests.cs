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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection
{
    public class SideEffectTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        [Fact]
        public void ShouldReturnCachedSideEffectWhenGetIsCalledAfterClose()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);
            var t = g.V().Aggregate("a").Iterate();

            t.SideEffects.Get("a");
            t.SideEffects.Close();
            var results = t.SideEffects.Get("a");

            Assert.NotNull(results);
        }

        [Fact]
        public void ShouldThrowWhenGetIsCalledAfterCloseAndNoSideEffectsAreCachec()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);
            var t = g.V().Aggregate("a").Iterate();

            t.SideEffects.Close();
            Assert.Throws<InvalidOperationException>(() => t.SideEffects.Get("a"));
        }

        [Fact]
        public void ShouldThrowWhenGetIsCalledAfterDisposeAndNoSideEffectsAreCachec()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);
            var t = g.V().Aggregate("a").Iterate();

            t.SideEffects.Dispose();
            Assert.Throws<InvalidOperationException>(() => t.SideEffects.Get("a"));
        }

        [Fact]
        public void ShouldReturnSideEffectValueWhenGetIsCalledForGroupCountTraversal()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);
            var t = g.V().Out("created").GroupCount("m").By("name").Iterate();
            t.SideEffects.Keys();
            var m = (IList) t.SideEffects.Get("m");

            Assert.Equal(2, m.Count);
            var result = new Dictionary<object, object>();
            
            foreach (IDictionary map in m)
            {
                foreach (var key in map.Keys)
                {
                    result.Add(key, map[key]);
                }
            }
            Assert.Equal((long) 3, result["lop"]);
            Assert.Equal((long) 1, result["ripple"]);
        }

        [Fact]
        public void ShouldReturnSideEffectValueWhenGetIsCalledOnATraversalWithSideEffect()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);
            var t = g.WithSideEffect("a", new List<string> {"first", "second"}).V().Iterate();
            t.SideEffects.Keys();

            var a = t.SideEffects.Get("a") as List<object>;

            Assert.Equal(2, a.Count);
            Assert.Equal("first", a[0]);
            Assert.Equal("second", a[1]);
        }

        [Fact]
        public void ShouldThrowWhenGetIsCalledWithAnUnknownKey()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);
            var t = g.V().Iterate();

            Assert.Throws<KeyNotFoundException>(() => t.SideEffects.Get("m"));
        }

        [Fact]
        public void ShouldReturnBothSideEffectForTraversalWithTwoSideEffects_()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);

            var t = g.V().Out("created").GroupCount("m").By("name").Values<string>("name").Aggregate("n").Iterate();

            var keys = t.SideEffects.Keys().ToList();
            Assert.Equal(2, keys.Count);
            Assert.Contains("m", keys);
            Assert.Contains("n", keys);
            var n = (IList<object>) t.SideEffects.Get("n");
            Assert.Equal(n.Select(tr => ((Traverser)tr).Object), new[] {"lop", "ripple"});
        }

        [Fact]
        public void ShouldReturnAnEmptyCollectionWhenKeysIsCalledForTraversalWithoutSideEffect()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);

            var t = g.V().Iterate();
            var keys = t.SideEffects.Keys();

            Assert.Equal(0, keys.Count);
        }

        [Fact]
        public void ShouldReturnCachedKeysWhenForCloseAfterSomeGet()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);
            var t = g.V().Aggregate("a").Aggregate("b").Iterate();

            t.SideEffects.Get("a");
            t.SideEffects.Close();
            var keys = t.SideEffects.Keys();

            Assert.Equal(2, keys.Count);
            Assert.Contains("a", keys);
            Assert.Contains("b", keys);
        }

        [Fact]
        public void ShouldReturnSideEffectKeyWhenKeysIsCalledForNamedGroupCount()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);
            var t = g.V().Out("created").GroupCount("m").By("name").Iterate();

            var keys = t.SideEffects.Keys();

            var keysList = keys.ToList();
            Assert.Equal(1, keysList.Count);
            Assert.Contains("m", keysList);
        }

        [Fact]
        public async Task ShouldReturnSideEffectsKeysWhenKeysIsCalledOnTraversalThatExecutedAsynchronously()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);

            var t = await g.V().Aggregate("a").Promise(x => x);
            var keys = t.SideEffects.Keys();

            Assert.Equal(1, keys.Count);
            Assert.Contains("a", keys);
        }

        [Fact]
        public async Task ShouldReturnSideEffectValueWhenGetIsCalledOnTraversalThatExecutedAsynchronously()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);

            var t = await g.V().Aggregate("a").Promise(x => x);
            var value = t.SideEffects.Get("a");

            Assert.NotNull(value);
        }

        [Fact]
        public async Task ShouldNotThrowWhenCloseIsCalledOnTraversalThatExecutedAsynchronously()
        {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = graph.Traversal().WithRemote(connection);

            var t = await g.V().Aggregate("a").Promise(x => x);
            t.SideEffects.Close();
        }
    }
}