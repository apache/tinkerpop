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
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphSON;
using Moq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphSON
{
    public class GraphSONReaderTests
    {
        private GraphSONReader CreateStandardGraphSONReader()
        {
            return new GraphSONReader();
        }

        [Fact]
        public void ShouldDeserializeWithCustomDeserializerForNewType()
        {
            var deserializerByGraphSONType = new Dictionary<string, IGraphSONDeserializer>
            {
                {"NS:TestClass", new TestGraphSONDeserializer()}
            };
            var reader = new GraphSONReader(deserializerByGraphSONType);
            var graphSON = "{\"@type\":\"NS:TestClass\",\"@value\":\"test\"}";

            var jObject = JObject.Parse(graphSON);
            var readObj = reader.ToObject(jObject);

            Assert.Equal("test", readObj.Value);
        }

        [Fact]
        public void ShouldDeserializeWithCustomDeserializerForCommonType()
        {
            var customSerializerMock = new Mock<IGraphSONDeserializer>();
            var overrideTypeString = "g:Int64";
            var customSerializerByType = new Dictionary<string, IGraphSONDeserializer>
            {
                {overrideTypeString, customSerializerMock.Object}
            };
            var reader = new GraphSONReader(customSerializerByType);


            reader.ToObject(JObject.Parse($"{{\"@type\":\"{overrideTypeString}\",\"@value\":12}}"));

            customSerializerMock.Verify(m => m.Objectify(It.IsAny<JToken>(), It.IsAny<GraphSONReader>()));
        }

        [Fact]
        public void ShouldDeserializeDateToDateTime()
        {
            var graphSon = "{\"@type\":\"g:Date\",\"@value\":1475583442552}";
            var reader = CreateStandardGraphSONReader();

            DateTime readDateTime = reader.ToObject(JObject.Parse(graphSon));

            var expectedDateTime = TestUtils.FromJavaTime(1475583442552);
            Assert.Equal(expectedDateTime, readDateTime);
        }

        [Fact]
        public void ShouldDeserializeDictionary()
        {
            var serializedDict = "{\"age\":[{\"@type\":\"g:Int32\",\"@value\":29}],\"name\":[\"marko\"]}";
            var reader = CreateStandardGraphSONReader();

            var jObject = JObject.Parse(serializedDict);
            var deserializedDict = reader.ToObject(jObject);

            var expectedDict = new Dictionary<string, dynamic>
            {
                {"age", new List<object> {29}},
                {"name", new List<object> {"marko"}}
            };
            Assert.Equal(expectedDict, deserializedDict);
        }

        [Fact]
        public void ShouldDeserializeEdge()
        {
            var graphSon =
                "{\"@type\":\"g:Edge\", \"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":17},\"label\":\"knows\",\"inV\":\"x\",\"outV\":\"y\",\"inVLabel\":\"xLab\",\"properties\":{\"aKey\":\"aValue\",\"bKey\":true}}}";
            var reader = CreateStandardGraphSONReader();

            Edge readEdge = reader.ToObject(JObject.Parse(graphSon));

            Assert.Equal((long) 17, readEdge.Id);
            Assert.Equal("knows", readEdge.Label);
            Assert.Equal(new Vertex("x", "xLabel"), readEdge.InV);
            Assert.Equal(new Vertex("y"), readEdge.OutV);
        }

        [Fact]
        public void ShouldDeserializeInt()
        {
            var serializedValue = "{\"@type\":\"g:Int32\",\"@value\":5}";
            var reader = CreateStandardGraphSONReader();

            var jObject = JObject.Parse(serializedValue);
            var deserializedValue = reader.ToObject(jObject);

            Assert.Equal(5, deserializedValue);
        }

        [Fact]
        public void ShouldDeserializeLong()
        {
            var serializedValue = "{\"@type\":\"g:Int64\",\"@value\":5}";
            var reader = CreateStandardGraphSONReader();

            var jObject = JObject.Parse(serializedValue);
            var deserializedValue = reader.ToObject(jObject);

            Assert.Equal((long) 5, deserializedValue);
        }

        [Fact]
        public void ShouldDeserializeFloat()
        {
            var serializedValue = "{\"@type\":\"g:Float\",\"@value\":31.3}";
            var reader = CreateStandardGraphSONReader();

            var jObject = JObject.Parse(serializedValue);
            var deserializedValue = reader.ToObject(jObject);

            Assert.Equal((float) 31.3, deserializedValue);
        }

        [Fact]
        public void ShouldDeserializeDouble()
        {
            var serializedValue = "{\"@type\":\"g:Double\",\"@value\":31.2}";
            var reader = CreateStandardGraphSONReader();

            var jObject = JObject.Parse(serializedValue);
            var deserializedValue = reader.ToObject(jObject);

            Assert.Equal(31.2, deserializedValue);
        }

        [Fact]
        public void ShouldDeserializeDecimal()
        {
            var serializedValue = "{\"@type\":\"gx:BigDecimal\",\"@value\":-8.201}";
            var reader = CreateStandardGraphSONReader();

            var jObject = JObject.Parse(serializedValue);
            decimal deserializedValue = reader.ToObject(jObject);

            Assert.Equal(-8.201M, deserializedValue);
        }

        [Fact]
        public void ShouldDeserializeDecimalValueAsString()
        {
            var serializedValue = "{\"@type\":\"gx:BigDecimal\",\"@value\":\"7.50\"}";
            var reader = CreateStandardGraphSONReader();

            var jObject = JObject.Parse(serializedValue);
            decimal deserializedValue = reader.ToObject(jObject);

            Assert.Equal(7.5M, deserializedValue);
        }

        [Fact]
        public void ShouldDeserializeList()
        {
            var serializedValue = "[{\"@type\":\"g:Int32\",\"@value\":5},{\"@type\":\"g:Int32\",\"@value\":6}]";
            var reader = CreateStandardGraphSONReader();

            var jObject = JArray.Parse(serializedValue);
            var deserializedValue = reader.ToObject(jObject);

            Assert.Equal(new List<object> {5, 6}, deserializedValue);
        }

        [Fact]
        public void ShouldDeserializePath()
        {
            var graphSon =
                "{\"@type\":\"g:Path\",\"@value\":{\"labels\":[[\"a\"],[\"b\",\"c\"],[]],\"objects\":[{\"@type\":\"g:Vertex\",\"@value\":{\"id\":{\"@type\":\"g:Int32\",\"@value\":1},\"label\":\"person\",\"properties\":{\"name\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":0},\"value\":\"marko\",\"label\":\"name\"}}],\"age\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":1},\"value\":{\"@type\":\"g:Int32\",\"@value\":29},\"label\":\"age\"}}]}}},{\"@type\":\"g:Vertex\",\"@value\":{\"id\":{\"@type\":\"g:Int32\",\"@value\":3},\"label\":\"software\",\"properties\":{\"name\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":4},\"value\":\"lop\",\"label\":\"name\"}}],\"lang\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":5},\"value\":\"java\",\"label\":\"lang\"}}]}}},\"lop\"]}}";
            var reader = CreateStandardGraphSONReader();

            Path readPath = reader.ToObject(JObject.Parse(graphSon));

            Assert.Equal("[v[1], v[3], lop]", readPath.ToString());
            Assert.Equal(new Vertex(1), readPath[0]);
            Assert.Equal(new Vertex(1), readPath["a"]);
            Assert.Equal("lop", readPath[2]);
            Assert.Equal(3, readPath.Count);
        }

        [Fact]
        public void ShouldDeserializePropertyWithEdgeElement()
        {
            var graphSon =
                "{\"@type\":\"g:Property\",\"@value\":{\"key\":\"aKey\",\"value\":{\"@type\":\"g:Int64\",\"@value\":17},\"element\":{\"@type\":\"g:Edge\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":122},\"label\":\"knows\",\"inV\":\"x\",\"outV\":\"y\",\"inVLabel\":\"xLab\"}}}}";
            var reader = CreateStandardGraphSONReader();

            Property readProperty = reader.ToObject(JObject.Parse(graphSon));

            Assert.Equal("aKey", readProperty.Key);
            Assert.Equal((long) 17, readProperty.Value);
            Assert.Equal(typeof(Edge), readProperty.Element.GetType());
            var edge = readProperty.Element as Edge;
            Assert.Equal((long) 122, edge.Id);
            Assert.Equal("knows", edge.Label);
            Assert.Equal("x", edge.InV.Id);
            Assert.Equal("y", edge.OutV.Id);
        }

        [Fact]
        public void ShouldDeserializeTimestampToDateTime()
        {
            var graphSon = "{\"@type\":\"g:Timestamp\",\"@value\":1475583442558}";
            var reader = CreateStandardGraphSONReader();

            DateTime readDateTime = reader.ToObject(JObject.Parse(graphSon));

            var expectedDateTime = TestUtils.FromJavaTime(1475583442558);
            Assert.Equal(expectedDateTime, readDateTime);
        }

        [Fact]
        public void ShouldDeserializeGuid()
        {
            var graphSon = "{\"@type\":\"g:UUID\",\"@value\":\"41d2e28a-20a4-4ab0-b379-d810dede3786\"}";
            var reader = CreateStandardGraphSONReader();

            Guid readGuid = reader.ToObject(JObject.Parse(graphSon));

            Assert.Equal(Guid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786"), readGuid);
        }

        [Fact]
        public void ShouldDeserializeVertexProperty()
        {
            var graphSon =
                "{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":\"anId\",\"label\":\"aKey\",\"value\":true,\"vertex\":{\"@type\":\"g:Int32\",\"@value\":9}}}";
            var reader = CreateStandardGraphSONReader();

            VertexProperty readVertexProperty = reader.ToObject(JObject.Parse(graphSon));

            Assert.Equal("anId", readVertexProperty.Id);
            Assert.Equal("aKey", readVertexProperty.Label);
            Assert.True(readVertexProperty.Value);
            Assert.NotNull(readVertexProperty.Vertex);
        }

        [Fact]
        public void ShouldDeserializeVertexPropertyWithLabel()
        {
            var graphSon =
                "{\"@type\":\"g:VertexProperty\", \"@value\":{\"id\":{\"@type\":\"g:Int32\",\"@value\":1},\"label\":\"name\",\"value\":\"marko\"}}";
            var reader = CreateStandardGraphSONReader();

            VertexProperty readVertexProperty = reader.ToObject(JObject.Parse(graphSon));

            Assert.Equal(1, readVertexProperty.Id);
            Assert.Equal("name", readVertexProperty.Label);
            Assert.Equal("marko", readVertexProperty.Value);
            Assert.Null(readVertexProperty.Vertex);
        }

        [Fact]
        public void ShouldDeserializeVertex()
        {
            var graphSon = "{\"@type\":\"g:Vertex\", \"@value\":{\"id\":{\"@type\":\"g:Float\",\"@value\":45.23}}}";
            var reader = CreateStandardGraphSONReader();

            var readVertex = reader.ToObject(JObject.Parse(graphSon));

            Assert.Equal(new Vertex(45.23f), readVertex);
        }

        [Fact]
        public void ShouldDeserializeVertexWithEdges()
        {
            var graphSon =
                "{\"@type\":\"g:Vertex\", \"@value\":{\"id\":{\"@type\":\"g:Int32\",\"@value\":1},\"label\":\"person\",\"outE\":{\"created\":[{\"id\":{\"@type\":\"g:Int32\",\"@value\":9},\"inV\":{\"@type\":\"g:Int32\",\"@value\":3},\"properties\":{\"weight\":{\"@type\":\"g:Double\",\"@value\":0.4}}}],\"knows\":[{\"id\":{\"@type\":\"g:Int32\",\"@value\":7},\"inV\":{\"@type\":\"g:Int32\",\"@value\":2},\"properties\":{\"weight\":{\"@type\":\"g:Double\",\"@value\":0.5}}},{\"id\":{\"@type\":\"g:Int32\",\"@value\":8},\"inV\":{\"@type\":\"g:Int32\",\"@value\":4},\"properties\":{\"weight\":{\"@type\":\"g:Double\",\"@value\":1.0}}}]},\"properties\":{\"name\":[{\"id\":{\"@type\":\"g:Int64\",\"@value\":0},\"value\":\"marko\"}],\"age\":[{\"id\":{\"@type\":\"g:Int64\",\"@value\":1},\"value\":{\"@type\":\"g:Int32\",\"@value\":29}}]}}}";
            var reader = CreateStandardGraphSONReader();

            var readVertex = reader.ToObject(JObject.Parse(graphSon));

            Assert.Equal(new Vertex(1), readVertex);
            Assert.Equal("person", readVertex.Label);
            Assert.Equal(typeof(int), readVertex.Id.GetType());
        }

        [Fact]
        public void ShouldDeserializeTraverser()
        {
            dynamic d = JObject.Parse("{\"@type\":\"g:Traverser\",\"@value\":1}");

            Assert.NotNull(d);
            Assert.Equal("g:Traverser", (string)d["@type"]);
        }
    }

    internal class TestGraphSONDeserializer : IGraphSONDeserializer
    {
        public dynamic Objectify(JToken graphsonObject, GraphSONReader reader)
        {
            return new TestClass {Value = graphsonObject.ToString()};
        }
    }
}