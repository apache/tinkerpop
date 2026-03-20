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

using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Gremlin.Net.Structure.IO.GraphSON;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
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
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var vertex = g.V(1).Next();

            VerifyVertexProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldRespectMaterializePropertiesTokensForBytecode(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gmodern", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var vertex = g.With(Tokens.ArgMaterializeProperties, "tokens").V(1).Next();

            VerifyEmptyProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldRespectMaterializePropertiesAllForBytecode(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gmodern", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var vertex = g.With(Tokens.ArgMaterializeProperties, "all").V(1).Next();

            VerifyVertexPropertiesAtLeast(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldHandleEmptyVertexPropertiesForBytecode(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gimmutable", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var vertex = g.AddV("test").Next();

            VerifyEmptyProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldDeserializeVertexPropertiesForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var requestMsg = RequestMessage.Build("g.V(1)").AddG("gmodern").Create();
            var vertex = await client.SubmitWithSingleResultAsync<Vertex>(requestMsg);

            VerifyVertexProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldHandleEmptyVertexPropertiesForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var requestMsg = RequestMessage.Build("g.addV('test')").AddG("gimmutable").Create();
            var vertex = await client.SubmitWithSingleResultAsync<Vertex>(requestMsg);

            VerifyEmptyProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldRespectMaterializePropertiesAllForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var requestMsg = RequestMessage.Build("g.with('materializeProperties', 'all').V(1)").AddG("gmodern").Create();
            var vertex = await client.SubmitWithSingleResultAsync<Vertex>(requestMsg);

            VerifyVertexPropertiesAtLeast(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldRespectMaterializePropertiesTokensForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var requestMsg = RequestMessage.Build("g.with('materializeProperties', 'tokens').V(1)").AddG("gmodern").Create();
            var vertex = await client.SubmitWithSingleResultAsync<Vertex>(requestMsg);

            VerifyEmptyProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldDeserializeEdgePropertiesForBytecode(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gmodern", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var edge = g.E(7).Next();

            VerifyEdgeProperties(edge);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldHandleEmptyEdgePropertiesForBytecode(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gimmutable", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var v1 = g.AddV("v1").Next();
            var v2 = g.AddV("v2").Next();
            var edge = g.AddE("test").From(__.V(v1!.Id)).To(__.V(v2!.Id)).Next();

            VerifyEmptyProperties(edge);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldDeserializeEdgePropertiesForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var requestMsg = RequestMessage.Build("g.E(7)").AddG("gmodern").Create();
            var edge = await client.SubmitWithSingleResultAsync<Edge>(requestMsg);

            VerifyEdgeProperties(edge);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldHandleEmptyEdgePropertiesForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var requestMsg = RequestMessage.Build("g.addV().as('v1').addV().as('v2').addE('test').from('v1').to('v2')").AddG("gimmutable").Create();
            var edge = await client.SubmitWithSingleResultAsync<Edge>(requestMsg);

            VerifyEmptyProperties(edge);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldHandleMultiplePropertiesWithSameNameForVertex(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gimmutable", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var vertex = g.AddV()
                .Property(Cardinality.List, "test", "value1")
                .Property(Cardinality.List, "test", "value2")
                .Property(Cardinality.List, "test", "value3")
                .Next()!;

            vertex = g.V(vertex.Id).Next();

            Assert.NotNull(vertex);

            var properties = vertex.Properties!;
            Assert.Equal(3, properties.Length);
            var propertyValues = properties.Cast<VertexProperty>().Where(p => p.Key == "test").Select(p => p.Value).ToArray();
            Assert.Equal(3, propertyValues.Length);
            Assert.Equal(new[] { "value1", "value2", "value3" }, propertyValues);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public void ShouldDeserializeVertexVertexPropertiesForBytecode(IMessageSerializer serializer)
        {
            var connection = _connectionFactory.CreateRemoteConnection("gcrew", 2, serializer);
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var vertex = g.V(7).Next();

            VerifyVertexVertexProperties(vertex);
        }

        [Theory]
        [MemberData(nameof(Serializers))]
        public async Task ShouldDeserializeVertexVertexPropertiesForGremlin(IMessageSerializer serializer)
        {
            var client = _connectionFactory.CreateClient(serializer);

            var requestMsg = RequestMessage.Build("g.V(7)").AddG("gcrew").Create();
            var vertex = await client.SubmitWithSingleResultAsync<Vertex>(requestMsg);

            VerifyVertexVertexProperties(vertex);
        }

        private static void VerifyVertexProperties(Vertex? vertex)
        {
            Assert.NotNull(vertex);
            Assert.Equal(1, vertex.Id);
            Assert.Equal("person", vertex.Label);
            Assert.True(2 == vertex.Properties!.Length, $"Unexpected properties count: {JsonSerializer.Serialize(vertex.Properties)}");

            var age = vertex.Property("age");
            Assert.NotNull(age);
            Assert.Equal(29, age.Value);
        }

        /// <summary>
        ///     Verifies vertex has at least the expected user properties. Used for materializeProperties=all
        ///     which may include additional system properties like gremlin.traversalVertexProgram.haltedTraversers.
        /// </summary>
        private static void VerifyVertexPropertiesAtLeast(Vertex? vertex)
        {
            Assert.NotNull(vertex);
            Assert.Equal(1, vertex.Id);
            Assert.Equal("person", vertex.Label);
            Assert.True(vertex.Properties!.Length >= 2, $"Expected at least 2 properties, got: {JsonSerializer.Serialize(vertex.Properties)}");

            var age = vertex.Property("age");
            Assert.NotNull(age);
            Assert.Equal(29, age.Value);

            var name = vertex.Property("name");
            Assert.NotNull(name);
            Assert.Equal("marko", name.Value);
        }

        private static void VerifyVertexVertexProperties(Vertex? vertex)
        {
            Assert.NotNull(vertex);
            Assert.Equal(7, vertex.Id);
            Assert.Equal("person", vertex.Label);
            Assert.True(4 == vertex.Properties!.Length, $"Unexpected properties count: {JsonSerializer.Serialize(vertex.Properties)}");

            var locations = vertex.Properties.Cast<VertexProperty>().Where(p => p.Key == "location");
            Assert.NotNull(locations);
            Assert.Equal(3, locations.Count());

            var vertexProperty = locations.First();
            Assert.Equal("centreville", vertexProperty.Value);
            Assert.Equal(2, vertexProperty.Properties!.Length);

            var vertexPropertyPropertyStartTime = vertexProperty.Property("startTime");
            Assert.Equal(1990, vertexPropertyPropertyStartTime!.Value);

            var vertexPropertyPropertyEndTime = vertexProperty.Property("endTime");
            Assert.Equal(2000, vertexPropertyPropertyEndTime!.Value);
        }

        private static void VerifyEdgeProperties(Edge? edge)
        {
            Assert.NotNull(edge);
            Assert.Equal(7, edge.Id);
            Assert.Equal("knows", edge.Label);
            Assert.True(1 == edge.Properties!.Length, $"Unexpected properties count: {JsonSerializer.Serialize(edge.Properties)}");

            var weight = edge.Property("weight");
            Assert.NotNull(weight);
            Assert.Equal(0.5, weight.Value);
        }

        private static void VerifyEmptyProperties(Element? element)
        {
            Assert.NotNull(element);
            Assert.True((element.Properties?.Length ?? 0) == 0);
        }

        // TODO: Remove GraphSON serializer tests entirely once GraphSON support is fully removed from the driver.
        // GraphSON 2/3 serializers are not compatible with the HTTP 4.0 transport (they use WebSocket-style
        // MIME-type framing and the server only accepts GraphBinary 4.0 or GraphSON 4.0 over HTTP).
        public static List<object[]> Serializers => new()
        {
            new object[] { new GraphBinary4MessageSerializer() }
        };

        [Fact(Skip = "GraphSON 2/3 serializers are not supported over HTTP 4.0 transport")]
        public void GraphSONSerializersNotSupportedOverHttp()
        {
            // TODO: Remove this test and the GraphSON serializer references when GraphSON is fully removed.
            // GraphSON2MessageSerializer and GraphSON3MessageSerializer use WebSocket-style MIME-type framing
            // that is incompatible with the HTTP channelizer. The server only accepts GraphBinary 4.0 or
            // GraphSON 4.0 (application/json) over HTTP.
        }
    }
}
