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
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphSON;
using Moq;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphSON
{
    public class GraphSONWriterTests
    {
        private GraphSONWriter CreateStandardGraphSONWriter()
        {
            return new GraphSONWriter();
        }

        [Fact]
        public void ShouldSerializeInt()
        {
            var writer = CreateStandardGraphSONWriter();

            var graphSon = writer.WriteObject(1);

            Assert.Equal("{\"@type\":\"g:Int32\",\"@value\":1}", graphSon);
        }

        [Fact]
        public void ShouldSerializeLong()
        {
            var writer = CreateStandardGraphSONWriter();

            var graphSon = writer.WriteObject((long) 2);

            Assert.Equal("{\"@type\":\"g:Int64\",\"@value\":2}", graphSon);
        }

        [Fact]
        public void ShouldSerializeFloat()
        {
            var writer = CreateStandardGraphSONWriter();

            var graphSon = writer.WriteObject((float) 3.2);

            Assert.Equal("{\"@type\":\"g:Float\",\"@value\":3.2}", graphSon);
        }

        [Fact]
        public void ShouldSerializeDouble()
        {
            var writer = CreateStandardGraphSONWriter();

            var graphSon = writer.WriteObject(3.2);

            Assert.Equal("{\"@type\":\"g:Double\",\"@value\":3.2}", graphSon);
        }

        [Fact]
        public void ShouldSerializeDecimal()
        {
            var writer = CreateStandardGraphSONWriter();

            var graphSon = writer.WriteObject(6.5M);

            Assert.Equal("{\"@type\":\"gx:BigDecimal\",\"@value\":\"6.5\"}", graphSon);
        }

        [Fact]
        public void ShouldSerializeBoolean()
        {
            var writer = CreateStandardGraphSONWriter();

            var graphSon = writer.WriteObject(true);

            Assert.Equal("true", graphSon);
        }

        [Fact]
        public void ShouldSerializeArray()
        {
            var writer = CreateStandardGraphSONWriter();
            var array = new[] {5, 6};

            var serializedGraphSON = writer.WriteObject(array);

            var expectedGraphSON = "[{\"@type\":\"g:Int32\",\"@value\":5},{\"@type\":\"g:Int32\",\"@value\":6}]";
            Assert.Equal(expectedGraphSON, serializedGraphSON);
        }

        [Fact]
        public void ShouldSerializeBinding()
        {
            var writer = CreateStandardGraphSONWriter();
            var binding = new Binding("theKey", 123);

            var graphSon = writer.WriteObject(binding);

            const string expected =
                "{\"@type\":\"g:Binding\",\"@value\":{\"value\":{\"@type\":\"g:Int32\",\"@value\":123},\"key\":\"theKey\"}}";
            Assert.Equal(expected, graphSon);
        }

        [Fact]
        public void ShouldSerializeWithCustomSerializerForNewType()
        {
            var customSerializerByType = new Dictionary<Type, IGraphSONSerializer>
            {
                {typeof(TestClass), new TestGraphSONSerializer {TestNamespace = "NS"}}
            };
            var writer = new GraphSONWriter(customSerializerByType);
            var testObj = new TestClass {Value = "test"};

            var serialized = writer.WriteObject(testObj);

            Assert.Equal("{\"@type\":\"NS:TestClass\",\"@value\":\"test\"}", serialized);
        }

        [Fact]
        public void ShouldSerializeWithCustomSerializerForCommonType()
        {
            var customSerializerMock = new Mock<IGraphSONSerializer>();
            var customSerializerByType = new Dictionary<Type, IGraphSONSerializer>
            {
                {typeof(int), customSerializerMock.Object}
            };
            var writer = new GraphSONWriter(customSerializerByType);

            writer.WriteObject(12);

            customSerializerMock.Verify(m => m.Dictify(It.Is<int>(v => v == 12), It.IsAny<GraphSONWriter>()));
        }

        [Fact]
        public void ShouldSerializeDateTimeOffset()
        {
            var writer = CreateStandardGraphSONWriter();
            var dateTimeOffset = TestUtils.FromJavaTime(1475583442552);

            var graphSon = writer.WriteObject(dateTimeOffset);

            const string expected = "{\"@type\":\"g:Date\",\"@value\":1475583442552}";
            Assert.Equal(expected, graphSon);
        }

        [Fact]
        public void ShouldSerializeDictionary()
        {
            var writer = CreateStandardGraphSONWriter();
            var dictionary = new Dictionary<string, dynamic>
            {
                {"age", new List<int> {29}},
                {"name", new List<string> {"marko"}}
            };

            var serializedDict = writer.WriteObject(dictionary);

            var expectedGraphSON = "{\"age\":[{\"@type\":\"g:Int32\",\"@value\":29}],\"name\":[\"marko\"]}";
            Assert.Equal(expectedGraphSON, serializedDict);
        }

        [Fact]
        public void ShouldSerializeEdge()
        {
            var writer = CreateStandardGraphSONWriter();
            var edge = new Edge(7, new Vertex(0, "person"), "knows", new Vertex(1, "dog"));

            var graphSON = writer.WriteObject(edge);

            const string expected =
                "{\"@type\":\"g:Edge\",\"@value\":{\"id\":{\"@type\":\"g:Int32\",\"@value\":7},\"outV\":{\"@type\":\"g:Int32\",\"@value\":0},\"outVLabel\":\"person\",\"label\":\"knows\",\"inV\":{\"@type\":\"g:Int32\",\"@value\":1},\"inVLabel\":\"dog\"}}";
            Assert.Equal(expected, graphSON);
        }

        [Fact]
        public void ShouldSerializeEnum()
        {
            var writer = CreateStandardGraphSONWriter();

            var serializedEnum = writer.WriteObject(Direction.Both);

            var expectedGraphSON = "{\"@type\":\"g:Direction\",\"@value\":\"BOTH\"}";
            Assert.Equal(expectedGraphSON, serializedEnum);
        }

        [Fact]
        public void ShouldSerializeList()
        {
            var writer = CreateStandardGraphSONWriter();
            var list = new List<int> {5, 6};

            var serializedGraphSON = writer.WriteObject(list.ToArray());

            var expectedGraphSON = "[{\"@type\":\"g:Int32\",\"@value\":5},{\"@type\":\"g:Int32\",\"@value\":6}]";
            Assert.Equal(expectedGraphSON, serializedGraphSON);
        }

        [Fact]
        public void ShouldSerializePredicateWithTwoValues()
        {
            var writer = CreateStandardGraphSONWriter();
            var predicate = new P("within", new List<int> {1, 2});

            var serializedPredicate = writer.WriteObject(predicate);

            var expectedGraphSON =
                "{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"within\",\"value\":[{\"@type\":\"g:Int32\",\"@value\":1},{\"@type\":\"g:Int32\",\"@value\":2}]}}";
            Assert.Equal(expectedGraphSON, serializedPredicate);
        }

        [Fact]
        public void ShouldSerializePredicateWithSingleValue()
        {
            var writer = CreateStandardGraphSONWriter();
            var predicate = new P("lt", 5);

            var serializedPredicate = writer.WriteObject(predicate);

            var expectedGraphSON =
                "{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"lt\",\"value\":{\"@type\":\"g:Int32\",\"@value\":5}}}";
            Assert.Equal(expectedGraphSON, serializedPredicate);
        }

        [Fact]
        public void ShouldSerializePropertyWithEdgeElement()
        {
            var writer = CreateStandardGraphSONWriter();
            var property = new Property("aKey", "aValue", new Edge("anId", new Vertex(1), "edgeLabel", new Vertex(2)));

            var graphSON = writer.WriteObject(property);

            const string expected =
                "{\"@type\":\"g:Property\",\"@value\":{\"key\":\"aKey\",\"value\":\"aValue\",\"element\":{\"@type\":\"g:Edge\",\"@value\":{\"id\":\"anId\",\"outV\":{\"@type\":\"g:Int32\",\"@value\":1},\"label\":\"edgeLabel\",\"inV\":{\"@type\":\"g:Int32\",\"@value\":2}}}}}";
            Assert.Equal(expected, graphSON);
        }

        [Fact]
        public void ShouldSerializePropertyWithVertexPropertyElement()
        {
            var writer = CreateStandardGraphSONWriter();
            var property = new Property("name", "marko",
                new VertexProperty("anId", "aKey", 21345, new Vertex("vertexId")));

            var graphSON = writer.WriteObject(property);

            const string expected =
                "{\"@type\":\"g:Property\",\"@value\":{\"key\":\"name\",\"value\":\"marko\",\"element\":{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":\"anId\",\"label\":\"aKey\",\"vertex\":\"vertexId\"}}}}";
            Assert.Equal(expected, graphSON);
        }

        [Fact]
        public void ShouldSerializeVertexProperty()
        {
            var writer = CreateStandardGraphSONWriter();
            var vertexProperty = new VertexProperty("blah", "keyA", true, new Vertex("stephen"));

            var graphSON = writer.WriteObject(vertexProperty);

            const string expected =
                "{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":\"blah\",\"label\":\"keyA\",\"value\":true,\"vertex\":\"stephen\"}}";
            Assert.Equal(expected, graphSON);
        }

        [Fact]
        public void ShouldSerializeGuid()
        {
            var writer = CreateStandardGraphSONWriter();
            var guid = Guid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786");

            var graphSon = writer.WriteObject(guid);

            const string expected = "{\"@type\":\"g:UUID\",\"@value\":\"41d2e28a-20a4-4ab0-b379-d810dede3786\"}";
            Assert.Equal(expected, graphSon);
        }

        [Fact]
        public void ShouldSerializeVertex()
        {
            var writer = CreateStandardGraphSONWriter();
            var vertex = new Vertex(45.23f);

            var graphSON = writer.WriteObject(vertex);

            const string expected =
                "{\"@type\":\"g:Vertex\",\"@value\":{\"id\":{\"@type\":\"g:Float\",\"@value\":45.23},\"label\":\"vertex\"}}";
            Assert.Equal(expected, graphSON);
        }

        [Fact]
        public void ShouldSerializeVertexWithLabel()
        {
            var writer = CreateStandardGraphSONWriter();
            var vertex = new Vertex((long) 123, "project");

            var graphSON = writer.WriteObject(vertex);

            const string expected =
                "{\"@type\":\"g:Vertex\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":123},\"label\":\"project\"}}";
            Assert.Equal(expected, graphSON);
        }

        [Fact]
        public void ShouldSerializeTypeToItsObject()
        {
            var writer = CreateStandardGraphSONWriter();
            var type = typeof(SubgraphStrategy);

            var graphSon = writer.WriteObject(type);

            const string expected = "{\"@type\":\"g:SubgraphStrategy\",\"@value\":{}}";
            Assert.Equal(expected, graphSon);
        }

        [Fact]
        public void ShouldSerializeLambda()
        {
            var writer = CreateStandardGraphSONWriter();
            var lambda = Lambda.Groovy("{ it.get() }");

            var graphSon = writer.WriteObject(lambda);

            const string expected =
                "{\"@type\":\"g:Lambda\",\"@value\":{\"script\":\"{ it.get() }\",\"language\":\"gremlin-groovy\",\"arguments\":-1}}";
            Assert.Equal(expected, graphSon);
        }

        [Fact]
        public void ShouldSerializeTimeSpan()
        {
            var writer = CreateStandardGraphSONWriter();
            var timeSpan = new TimeSpan(5, 4, 3, 2, 1);

            var graphSon = writer.WriteObject(timeSpan);

            const string expected = "{\"@type\":\"gx:Duration\",\"@value\":\"P5DT4H3M2.001S\"}";
            Assert.Equal(expected, graphSon);
        }

        [Fact]
        public void ShouldSerializeBigInteger()
        {
            var writer = CreateStandardGraphSONWriter();
            var bigInteger = BigInteger.Parse("123456789987654321123456789987654321");

            var graphSon = writer.WriteObject(bigInteger);

            const string expected = "{\"@type\":\"gx:BigInteger\",\"@value\":\"123456789987654321123456789987654321\"}";
            Assert.Equal(expected, graphSon);
        }

        [Fact]
        public void ShouldSerializeByte()
        {
            var writer = CreateStandardGraphSONWriter();

            var graphSon = writer.WriteObject((byte)1);

            Assert.Equal("{\"@type\":\"gx:Byte\",\"@value\":1}", graphSon);
        }

        [Fact]
        public void ShouldSerializeByteBuffer()
        {
            var writer = CreateStandardGraphSONWriter();

            var graphSon = writer.WriteObject(Convert.FromBase64String("c29tZSBieXRlcyBmb3IgeW91"));

            Assert.Equal("{\"@type\":\"gx:ByteBuffer\",\"@value\":\"c29tZSBieXRlcyBmb3IgeW91\"}", graphSon);
        }

        [Fact]
        public void ShouldSerializeChar()
        {
            var writer = CreateStandardGraphSONWriter();

            var graphSon = writer.WriteObject('x');

            Assert.Equal("{\"@type\":\"gx:Char\",\"@value\":\"x\"}", graphSon);
        }
    }

    internal class TestGraphSONSerializer : IGraphSONSerializer
    {
        public string TestNamespace { get; set; }

        public Dictionary<string, dynamic> Dictify(dynamic objectData, GraphSONWriter writer)
        {
            return GraphSONUtil.ToTypedValue(nameof(TestClass), objectData.Value, TestNamespace);
        }
    }
}