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

using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    public class GraphSerializerTests
    {
        [Fact]
        public async Task ShouldRoundTripGraphWithVerticesEdgesAndProperties()
        {
            // Round-trips a Graph with two vertices linked by a single edge; one vertex has a
            // vertex property with a meta-property, and the edge carries a weight property.
            var metaProperty = new Property("acl", "public");
            var nameProperty = new VertexProperty(4, "name", "marko", null, new object[] { metaProperty });
            var v1 = new Vertex(1, "person", new object[] { nameProperty });
            var v2 = new Vertex(2, "person");
            var weightProperty = new Property("weight", 0.5);
            var e1 = new Edge(3, v1, "knows", v2, new object[] { weightProperty });

            var graph = new Graph();
            graph.Vertices[v1.Id!] = v1;
            graph.Vertices[v2.Id!] = v2;
            graph.Edges[e1.Id!] = e1;

            var writer = new GraphBinaryWriter();
            var reader = new GraphBinaryReader();
            using var stream = new MemoryStream();
            await writer.WriteAsync(graph, stream);
            stream.Position = 0;
            var result = await reader.ReadAsync(stream);

            Assert.NotNull(result);
            var deserialized = Assert.IsType<Graph>(result);
            Assert.Equal(2, deserialized.Vertices.Count);
            Assert.Single(deserialized.Edges);

            var rv1 = deserialized.Vertices[1];
            Assert.Equal("person", rv1.Label);
            var rvProperties = rv1.Properties.Cast<VertexProperty>().ToList();
            Assert.Single(rvProperties);
            var rvp1 = rvProperties[0];
            Assert.Equal(4, rvp1.Id);
            Assert.Equal("name", rvp1.Label);
            Assert.Equal("marko", (string?)rvp1.Value);
            var metaProps = rvp1.Properties.Cast<Property>().ToList();
            Assert.Single(metaProps);
            Assert.Equal("acl", metaProps[0].Key);
            Assert.Equal("public", (string?)metaProps[0].Value);

            Assert.True(deserialized.Vertices.ContainsKey(2));
            Assert.Equal("person", deserialized.Vertices[2].Label);

            var re1 = deserialized.Edges[3];
            Assert.Equal("knows", re1.Label);
            Assert.Equal(1, re1.OutV.Id);
            Assert.Equal(2, re1.InV.Id);
            var edgeProps = re1.Properties.Cast<Property>().ToList();
            Assert.Single(edgeProps);
            Assert.Equal("weight", edgeProps[0].Key);
            Assert.Equal(0.5, (double?)edgeProps[0].Value);
        }

        [Fact]
        public async Task ShouldRoundTripEmptyGraph()
        {
            var graph = new Graph();

            var writer = new GraphBinaryWriter();
            var reader = new GraphBinaryReader();
            using var stream = new MemoryStream();
            await writer.WriteAsync(graph, stream);
            stream.Position = 0;
            var result = await reader.ReadAsync(stream);

            Assert.NotNull(result);
            var deserialized = Assert.IsType<Graph>(result);
            Assert.Empty(deserialized.Vertices);
            Assert.Empty(deserialized.Edges);
        }

        [Fact]
        public void ToStringShouldRenderEmptyGraphCounts()
        {
            var graph = new Graph();
            Assert.Equal("graph[vertices:0 edges:0]", graph.ToString());
        }

        [Fact]
        public void ToStringShouldRenderVerticesAndEdgesCounts()
        {
            var v1 = new Vertex(1, "person");
            var v2 = new Vertex(2, "person");
            var graph = new Graph();
            graph.Vertices[v1.Id!] = v1;
            graph.Vertices[v2.Id!] = v2;
            graph.Edges[3] = new Edge(3, v1, "knows", v2);
            Assert.Equal("graph[vertices:2 edges:1]", graph.ToString());
        }
    }
}
