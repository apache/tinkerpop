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
using System.Collections.Generic;
using System.Numerics;
using System.Text.Json;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphSON;
using Moq;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphSON
{
    public class GraphSONReaderTests
    {
        /// <summary>
        /// Parameters for each test supporting multiple versions of GraphSON
        /// </summary>
        public static IEnumerable<object[]> Versions => new []
        {
            new object[] { 2 },
            new object[] { 3 }
        };

        /// <summary>
        /// Parameters for each collections test supporting multiple versions of GraphSON
        /// </summary>
        public static IEnumerable<object[]> VersionsSupportingCollections => new []
        {
            new object[] { 3 }
        };

        private GraphSONReader CreateStandardGraphSONReader(int version)
        {
            if (version == 3)
            {
                return new GraphSON3Reader();
            }
            return new GraphSON2Reader();
        }

        [Fact]
        public void ShouldDeserializeWithCustomDeserializerForNewType()
        {
            var deserializerByGraphSONType = new Dictionary<string, IGraphSONDeserializer>
            {
                {"NS:TestClass", new TestGraphSONDeserializer()}
            };
            var reader = new GraphSON2Reader(deserializerByGraphSONType);
            const string graphSON = "{\"@type\":\"NS:TestClass\",\"@value\":\"test\"}";
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSON);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal("test", deserializedValue.Value);
        }
        
        [Fact]
        public void ShouldDeserializeWithCustomDeserializerForCommonType()
        {
            var customSerializerMock = new Mock<IGraphSONDeserializer>();
            const string overrideTypeString = "g:Int64";
            var customSerializerByType = new Dictionary<string, IGraphSONDeserializer>
            {
                {overrideTypeString, customSerializerMock.Object}
            };
            var reader = new GraphSON2Reader(customSerializerByType);

            var jsonElement =
                JsonSerializer.Deserialize<JsonElement>($"{{\"@type\":\"{overrideTypeString}\",\"@value\":12}}");
            var deserializedValue = reader.ToObject(jsonElement);

            customSerializerMock.Verify(m => m.Objectify(It.IsAny<JsonElement>(), It.IsAny<GraphSONReader>()));
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeDateToDateTimeOffset(int version)
        {
            const string graphSon = "{\"@type\":\"g:Date\",\"@value\":1475583442552}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            var expectedDateTimeOffset = TestUtils.FromJavaTime(1475583442552);
            Assert.Equal(expectedDateTimeOffset, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeDictionary(int version)
        {
            const string serializedDict = "{\"age\":[{\"@type\":\"g:Int32\", \"@value\":29}], \"name\":[\"marko\"], " +
                                          "\"gender\": null}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedDict);
            var deserializedValue = reader.ToObject(jsonElement);
        
            var expectedDict = new Dictionary<string, dynamic>
            {
                {"age", new List<object> {29}},
                {"name", new List<object> {"marko"}},
                {"gender", null}
            };
            Assert.Equal(expectedDict, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeEdge(int version)
        {
            const string graphSon = "{\"@type\":\"g:Edge\", \"@value\":{\"id\":{\"@type\":\"g:Int64\", \"@value\":17}, " +
                                    "\"label\":\"knows\", \"inV\":\"x\", \"outV\":\"y\", \"inVLabel\":\"xLab\", " +
                                    "\"properties\":{\"aKey\":\"aValue\", \"bKey\":true}}}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            Edge readEdge = reader.ToObject(jsonElement);
        
            Assert.Equal((long) 17, readEdge.Id);
            Assert.Equal("knows", readEdge.Label);
            Assert.Equal(new Vertex("x", "xLabel"), readEdge.InV);
            Assert.Equal(new Vertex("y"), readEdge.OutV);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeInt(int version)
        {
            var serializedValue = "{\"@type\":\"g:Int32\",\"@value\":5}";
            var reader = CreateStandardGraphSONReader(version);

            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(5, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeLong(int version)
        {
            const string serializedValue = "{\"@type\":\"g:Int64\",\"@value\":5}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal((long) 5, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeFloat(int version)
        {
            const string serializedValue = "{\"@type\":\"g:Float\",\"@value\":31.3}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal((float) 31.3, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeDouble(int version)
        {
            const string serializedValue = "{\"@type\":\"g:Double\",\"@value\":31.2}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(31.2, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeNaN(int version)
        {
            const string serializedValue = "{\"@type\":\"g:Double\",\"@value\":\"NaN\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(Double.NaN, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializePositiveInfinity(int version)
        {
            const string serializedValue = "{\"@type\":\"g:Double\",\"@value\":\"Infinity\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(double.PositiveInfinity, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeNegativeInfinity(int version)
        {
            const string serializedValue = "{\"@type\":\"g:Double\",\"@value\":\"-Infinity\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(double.NegativeInfinity, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeDecimal(int version)
        {
            const string serializedValue = "{\"@type\":\"gx:BigDecimal\",\"@value\":-8.201}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(-8.201M, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeDecimalValueAsString(int version)
        {
            const string serializedValue = "{\"@type\":\"gx:BigDecimal\",\"@value\":\"7.50\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            decimal deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(7.5M, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeByte(int version)
        {
            const string serializedValue = "{\"@type\":\"gx:Byte\",\"@value\":1}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(1, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeChar(int version)
        {
            const string serializedValue = "{\"@type\":\"gx:Char\",\"@value\":\"x\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal('x', deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeList(int version)
        {
            const string serializedValue = "[{\"@type\":\"g:Int32\", \"@value\":5}, {\"@type\":\"g:Int32\", " +
                                           "\"@value\":6}, null]";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(new List<object> {5, 6, null}, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeT(int version)
        {
            const string graphSon = "{\"@type\":\"g:T\",\"@value\":\"label\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            T readT = reader.ToObject(jsonElement);
        
            Assert.Equal(T.Label, readT);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeDirection(int version)
        {
            const string serializedValue = "{\"@type\":\"g:Direction\",\"@value\":\"OUT\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(serializedValue);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(Direction.Out, deserializedValue);
        }
        
        [Fact]
        public void ShouldDeserializePathFromGraphSON2()
        {
            const string graphSon =
                "{\"@type\":\"g:Path\",\"@value\":{\"labels\":[[\"a\"],[\"b\",\"c\"],[]],\"objects\":[{\"@type\":\"g:Vertex\",\"@value\":{\"id\":{\"@type\":\"g:Int32\",\"@value\":1},\"label\":\"person\",\"properties\":{\"name\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":0},\"value\":\"marko\",\"label\":\"name\"}}],\"age\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":1},\"value\":{\"@type\":\"g:Int32\",\"@value\":29},\"label\":\"age\"}}]}}},{\"@type\":\"g:Vertex\",\"@value\":{\"id\":{\"@type\":\"g:Int32\",\"@value\":3},\"label\":\"software\",\"properties\":{\"name\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":4},\"value\":\"lop\",\"label\":\"name\"}}],\"lang\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":5},\"value\":\"java\",\"label\":\"lang\"}}]}}},\"lop\"]}}";
            var reader = CreateStandardGraphSONReader(2);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            Path readPath = reader.ToObject(jsonElement);
        
            Assert.Equal("path[v[1], v[3], lop]", readPath.ToString());
            Assert.Equal(new Vertex(1), readPath[0]);
            Assert.Equal(new Vertex(1), readPath["a"]);
            Assert.Equal("lop", readPath[2]);
            Assert.Equal(3, readPath.Count);
        }
        
        [Fact]
        public void ShouldDeserializePathFromGraphSON3()
        {
            const string graphSon = "{\"@type\":\"g:Path\",\"@value\":{" +
                                    "\"labels\":{\"@type\":\"g:List\",\"@value\":[{\"@type\":\"g:Set\",\"@value\":[\"z\"]}]}," +
                                    "\"objects\":{\"@type\":\"g:List\",\"@value\":[{\"@type\":\"g:Vertex\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":5},\"label\":\"\"}}]}}}";
            var reader = CreateStandardGraphSONReader(3);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            Path readPath = reader.ToObject(jsonElement);
        
            Assert.Equal("path[v[5]]", readPath.ToString());
            Assert.Equal(new Vertex(5L), readPath[0]);
            Assert.Equal(new Vertex(5L), readPath["z"]);
            Assert.Equal(1, readPath.Count);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializePropertyWithEdgeElement(int version)
        {
            const string graphSon = "{\"@type\":\"g:Property\", \"@value\":{\"key\":\"aKey\", " +
                                    "\"value\":{\"@type\":\"g:Int64\", \"@value\":17}, " +
                                    "\"element\":{\"@type\":\"g:Edge\", \"@value\":{\"id\":{\"@type\":\"g:Int64\", " +
                                    "\"@value\":122}, \"label\":\"knows\", \"inV\":\"x\", \"outV\":\"y\", " +
                                    "\"inVLabel\":\"xLab\"}}}}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            Property readProperty = reader.ToObject(jsonElement);
        
            Assert.Equal("aKey", readProperty.Key);
            Assert.Equal((long) 17, readProperty.Value);
            Assert.Equal(typeof(Edge), readProperty.Element.GetType());
            var edge = readProperty.Element as Edge;
            Assert.Equal((long) 122, edge.Id);
            Assert.Equal("knows", edge.Label);
            Assert.Equal("x", edge.InV.Id);
            Assert.Equal("y", edge.OutV.Id);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeTimestampToDateTimeOffset(int version)
        {
            const string graphSon = "{\"@type\":\"g:Timestamp\",\"@value\":1475583442558}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            var expectedDateTimeOffset = TestUtils.FromJavaTime(1475583442558);
            Assert.Equal(expectedDateTimeOffset, deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeGuid(int version)
        {
            const string graphSon = "{\"@type\":\"g:UUID\",\"@value\":\"41d2e28a-20a4-4ab0-b379-d810dede3786\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(Guid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786"), deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeVertexProperty(int version)
        {
            const string graphSon = "{\"@type\":\"g:VertexProperty\", \"@value\":{\"id\":\"anId\", \"label\":\"aKey\", " +
                                    "\"value\":true, \"vertex\":{\"@type\":\"g:Int32\", \"@value\":9}}}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            VertexProperty readVertexProperty = reader.ToObject(jsonElement);
        
            Assert.Equal("anId", readVertexProperty.Id);
            Assert.Equal("aKey", readVertexProperty.Label);
            Assert.True(readVertexProperty.Value);
            Assert.NotNull(readVertexProperty.Vertex);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeVertexPropertyWithLabel(int version)
        {
            const string graphSon = "{\"@type\":\"g:VertexProperty\", \"@value\":{\"id\":{\"@type\":\"g:Int32\", " +
                                    "\"@value\":1}, \"label\":\"name\", \"value\":\"marko\"}}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            VertexProperty readVertexProperty = reader.ToObject(jsonElement);
        
            Assert.Equal(1, readVertexProperty.Id);
            Assert.Equal("name", readVertexProperty.Label);
            Assert.Equal("marko", readVertexProperty.Value);
            Assert.Null(readVertexProperty.Vertex);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeVertex(int version)
        {
            const string graphSon = "{\"@type\":\"g:Vertex\", \"@value\":{\"id\":{\"@type\":\"g:Float\", " +
                                    "\"@value\":45.23}}}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(new Vertex(45.23f), deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeVertexWithLabel(int version)
        {
            const string graphSon = "{\"@type\":\"g:Vertex\", \"@value\":{\"id\":{\"@type\":\"g:Float\", " +
                                    "\"@value\":45.23}, \"label\": \"person\"}}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            Vertex deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal("person", deserializedValue.Label);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeVertexWithEdges(int version)
        {
            const string graphSon =
                "{\"@type\":\"g:Vertex\", \"@value\":{\"id\":{\"@type\":\"g:Int32\",\"@value\":1},\"label\":\"person\",\"outE\":{\"created\":[{\"id\":{\"@type\":\"g:Int32\",\"@value\":9},\"inV\":{\"@type\":\"g:Int32\",\"@value\":3},\"properties\":{\"weight\":{\"@type\":\"g:Double\",\"@value\":0.4}}}],\"knows\":[{\"id\":{\"@type\":\"g:Int32\",\"@value\":7},\"inV\":{\"@type\":\"g:Int32\",\"@value\":2},\"properties\":{\"weight\":{\"@type\":\"g:Double\",\"@value\":0.5}}},{\"id\":{\"@type\":\"g:Int32\",\"@value\":8},\"inV\":{\"@type\":\"g:Int32\",\"@value\":4},\"properties\":{\"weight\":{\"@type\":\"g:Double\",\"@value\":1.0}}}]},\"properties\":{\"name\":[{\"id\":{\"@type\":\"g:Int64\",\"@value\":0},\"value\":\"marko\"}],\"age\":[{\"id\":{\"@type\":\"g:Int64\",\"@value\":1},\"value\":{\"@type\":\"g:Int32\",\"@value\":29}}]}}}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            Vertex readVertex = reader.ToObject(jsonElement);
        
            Assert.Equal(new Vertex(1), readVertex);
            Assert.Equal("person", readVertex.Label);
            Assert.Equal(typeof(int), readVertex.Id.GetType());
        }
        
        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldDeserializeEmptyGList(int version)
        {
            const string graphSon = "{\"@type\":\"g:List\", \"@value\": []}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
            
            Assert.Equal(new object[0], deserializedValue);
        }
        
        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldDeserializeGList(int version)
        {
            const string json = "{\"@type\":\"g:List\", \"@value\": [{\"@type\": \"g:Int32\", \"@value\": 1}," +
                                "{\"@type\": \"g:Int32\", \"@value\": 2}, {\"@type\": \"g:Int32\", \"@value\": 3}]}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(json);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal((IList<object>)new object[] { 1, 2, 3}, deserializedValue);
        }
        
        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldDeserializeGSet(int version)
        {
            const string graphSon = "{\"@type\":\"g:Set\", \"@value\": [{\"@type\": \"g:Int32\", \"@value\": 1}," +
                                    "{\"@type\": \"g:Int32\", \"@value\": 2}, {\"@type\": \"g:Int32\", \"@value\": 3}]}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal((ISet<object>)new HashSet<object>{ 1, 2, 3}, deserializedValue);
        }
        
        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldDeserializeGMap(int version)
        {
            const string json = "{\"@type\":\"g:Map\", \"@value\": [\"a\",{\"@type\": \"g:Int32\", \"@value\": 1}, " +
                                "\"b\", {\"@type\": \"g:Int32\", \"@value\": 2}]}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(json);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(new Dictionary<object, object>{ { "a", 1 }, { "b", 2 }}, deserializedValue);
        }
        
        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldDeserializeGMapWithNonStringKeys(int version)
        {
            const string json = "{\"@type\":\"g:Map\", \"@value\": [{\"@type\":\"g:Int32\", \"@value\":123}, \"red\"]}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(json);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(new Dictionary<object, object>{ { 123, "red" }}, deserializedValue);
        }
        
        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldDeserializeBulkSet(int version)
        {
            const string graphSon = "{\"@type\": \"g:BulkSet\", \"@value\": [" +
                                    "\"marko\", {\"@type\": \"g:Int64\", \"@value\": 1}, " +
                                    "\"josh\", {\"@type\": \"g:Int64\", \"@value\": 3}]}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(new List<object>{ "marko", "josh", "josh", "josh" }, deserializedValue);
        }
        
        [Fact]
        public void ShouldDeserializeBulkSetWithGraphSON3()
        {
            const string graphSon =
                "{\"@type\":\"g:List\",\"@value\":[{\"@type\":\"g:Traverser\",\"@value\":{\"bulk\":{\"@type\":\"g:Int64\",\"@value\":1},\"value\":{\"@type\":\"g:BulkSet\",\"@value\":[{\"@type\":\"g:Int64\",\"@value\":1},{\"@type\":\"g:Int64\",\"@value\":2},{\"@type\":\"g:Int64\",\"@value\":0},{\"@type\":\"g:Int64\",\"@value\":3},{\"@type\":\"g:Int64\",\"@value\":2},{\"@type\":\"g:Int64\",\"@value\":1},{\"@type\":\"g:Double\",\"@value\":1.0},{\"@type\":\"g:Int64\",\"@value\":2}]}}}]}";
            var reader = CreateStandardGraphSONReader(3);
            
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        }
        
        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldDeserializeTraverser(int version)
        {
            const string json = "{\"@type\":\"g:Traverser\", \"@value\":{\"bulk\":{\"@type\":\"g:Int64\", " +
                                "\"@value\":10}, \"value\":{\"@type\":\"g:Int32\", \"@value\":1}}}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(json);
            Traverser deserializedValue = reader.ToObject(jsonElement);
            
            Assert.Equal(10, deserializedValue.Bulk);
            Assert.Equal(1, deserializedValue.Object);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeDurationToTimeSpan(int version)
        {
            const string graphSon = "{\"@type\":\"gx:Duration\",\"@value\":\"PT120H\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(TimeSpan.FromDays(5), deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeBigInteger(int version)
        {
            var graphSon = "{\"@type\":\"gx:BigInteger\",\"@value\":123456789}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(BigInteger.Parse("123456789"), deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeBigIntegerValueAsString(int version)
        {
            var graphSon = "{\"@type\":\"gx:BigInteger\", \"@value\":\"123456789\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(BigInteger.Parse("123456789"), deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeReallyBigIntegerValue(int version)
        {
            const string graphSon = "{\"@type\":\"gx:BigInteger\", \"@value\":123456789987654321123456789987654321}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(BigInteger.Parse("123456789987654321123456789987654321"), deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeByteBuffer(int version)
        {
            const string graphSon = "{\"@type\":\"gx:ByteBuffer\", \"@value\":\"c29tZSBieXRlcyBmb3IgeW91\"}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(Convert.FromBase64String("c29tZSBieXRlcyBmb3IgeW91"), deserializedValue);
        }
        
        [Theory, MemberData(nameof(Versions))]
        public void ShouldDeserializeInt16(int version)
        {
            const string graphSon = "{\"@type\":\"gx:Int16\", \"@value\":100}";
            var reader = CreateStandardGraphSONReader(version);
        
            var jsonElement = JsonSerializer.Deserialize<JsonElement>(graphSon);
            var deserializedValue = reader.ToObject(jsonElement);
        
            Assert.Equal(100, deserializedValue);
        }
    }

    internal class TestGraphSONDeserializer : IGraphSONDeserializer
    {
        public dynamic Objectify(JsonElement graphsonObject, GraphSONReader reader)
        {
            return new TestClass {Value = graphsonObject.GetString()};
        }
    }
}