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

using Gremlin.Net.Driver;
using Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary;
using Gremlin.Net.Structure.IO.GraphSON;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class PropertyDeserializationTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new();

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldDeserializeVertexPropertiesForBytecode(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gmodern", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var vertex = g.V(1).Next();

            VerifyVertexProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldHandleEmptyVertexPropertiesForBytecode(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gimmutable", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var vertex = g.AddV("test").Next();

            VerifyEmptyProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldDeserializeVertexPropertiesForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var vertex = await client.SubmitWithSingleResultAsync<Vertex>("gmodern.V(1)");

            VerifyVertexProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldHandleEmptyVertexPropertiesForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var vertex = await client.SubmitWithSingleResultAsync<Vertex>("gimmutable.addV('test')");

            VerifyEmptyProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldDeserializeEdgePropertiesForBytecode(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gmodern", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var edge = g.E(7).Next();

            VerifyEdgeProperties(edge);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldHandleEmptyEdgePropertiesForBytecode(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gimmutable", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var v1 = g.AddV("v1").Next();
            var v2 = g.AddV("v2").Next();
            var edge = g.AddE("test").From(v1).To(v2).Next();

            VerifyEmptyProperties(edge);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldDeserializeEdgePropertiesForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var edge = await client.SubmitWithSingleResultAsync<Edge>("gmodern.E(7)");

            VerifyEdgeProperties(edge);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldHandleEmptyEdgePropertiesForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var edge = await client.SubmitWithSingleResultAsync<Edge>(
                "gimmutable.addV().as('v1').addV().as('v2').addE('test').from('v1').to('v2')");

            VerifyEmptyProperties(edge);
        }

        private static void VerifyVertexProperties(Vertex vertex)
        {
            Assert.NotNull(vertex);
            Assert.Equal(1, vertex.Id);

            var properties = vertex.GetPropertiesAsDictionary();
            Assert.Equal(2, properties.Count);
            Assert.True(properties.ContainsKey("age"));
            Assert.Equal(29, properties["age"].Single().Value);
        }

        private static void VerifyEdgeProperties(Edge edge)
        {
            Assert.NotNull(edge);
            Assert.Equal(7, edge.Id);

            var properties = edge.GetPropertiesAsDictionary();
            Assert.Single(properties);
            Assert.True(properties.ContainsKey("weight"));
            Assert.Equal(0.5, properties["weight"].Single().Value);
        }

        private static void VerifyEmptyProperties(Vertex vertex)
        {
            Assert.NotNull(vertex);
            Assert.True((vertex.GetPropertiesAsDictionary()?.Count ?? 0) == 0);
        }

        private static void VerifyEmptyProperties(Edge edge)
        {
            Assert.NotNull(edge);
            Assert.True((edge.GetPropertiesAsDictionary()?.Count ?? 0) == 0);
        }

        public static List<object[]> Serializers => new()
        {
            new [] { new GraphSON2MessageSerializer() },
            new [] { new GraphSON3MessageSerializer() },
            new [] { new GraphBinaryMessageSerializer() }
        };
    }
}
