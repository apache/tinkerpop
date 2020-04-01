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

        /// <summary>
        /// Parameters for each collections test supporting multiple versions of GraphSON
        /// </summary>
        public static IEnumerable<object[]> VersionsNotSupportingCollections => new []
        {
            new object[] { 2 }
        };

        private GraphSONWriter CreateGraphSONWriter(int version)
        {
            if (version == 3)
            {
                return new GraphSON3Writer();
            }
            return new GraphSON2Writer();
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeInt(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject(1);

            Assert.Equal("{\"@type\":\"g:Int32\",\"@value\":1}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeLong(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject((long) 2);

            Assert.Equal("{\"@type\":\"g:Int64\",\"@value\":2}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeFloat(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject((float) 3.2);

            Assert.Equal("{\"@type\":\"g:Float\",\"@value\":3.2}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeDouble(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject(3.2);

            Assert.Equal("{\"@type\":\"g:Double\",\"@value\":3.2}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeNaN(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject(Double.NaN);

            Assert.Equal("{\"@type\":\"g:Double\",\"@value\":\"NaN\"}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializePositiveInfinity(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject(Double.PositiveInfinity);

            Assert.Equal("{\"@type\":\"g:Double\",\"@value\":\"Infinity\"}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeNegativeInfinity(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject(Double.NegativeInfinity);

            Assert.Equal("{\"@type\":\"g:Double\",\"@value\":\"-Infinity\"}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeDecimal(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject(6.5M);

            Assert.Equal("{\"@type\":\"gx:BigDecimal\",\"@value\":\"6.5\"}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeBoolean(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject(true);

            Assert.Equal("true", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeArray(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var array = new[] {5, 6};

            var serializedGraphSON = writer.WriteObject(array);

            var expectedGraphSON = "[{\"@type\":\"g:Int32\",\"@value\":5},{\"@type\":\"g:Int32\",\"@value\":6}]";
            Assert.Equal(expectedGraphSON, serializedGraphSON);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeBinding(int version)
        {
            var writer = CreateGraphSONWriter(version);
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
            var writer = new GraphSON2Writer(customSerializerByType);
            var testObj = new TestClass {Value = "test"};

            var serialized = writer.WriteObject(testObj);

            Assert.Equal("{\"@type\":\"NS:TestClass\",\"@value\":\"test\"}", serialized);
        }

        [Fact]
        public void ShouldSerializeWithCustomSerializerForCommonType()
        {
            var customSerializerMock = new Mock<IGraphSONSerializer>();
            customSerializerMock.Setup(m => m.Dictify(It.IsAny<int>(), It.IsAny<GraphSONWriter>()))
                .Returns(new Dictionary<string, dynamic>());
            var customSerializerByType = new Dictionary<Type, IGraphSONSerializer>
            {
                {typeof(int), customSerializerMock.Object}
            };
            var writer = new GraphSON2Writer(customSerializerByType);

            writer.WriteObject(12);

            customSerializerMock.Verify(m => m.Dictify(It.Is<int>(v => v == 12), It.IsAny<GraphSONWriter>()));
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeDateTimeOffset(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var dateTimeOffset = TestUtils.FromJavaTime(1475583442552);

            var graphSon = writer.WriteObject(dateTimeOffset);

            const string expected = "{\"@type\":\"g:Date\",\"@value\":1475583442552}";
            Assert.Equal(expected, graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeDictionary(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var dictionary = new Dictionary<string, dynamic>
            {
                {"age", new List<int> {29}},
                {"name", new List<string> {"marko"}}
            };

            var serializedDict = writer.WriteObject(dictionary);

            var expectedGraphSON = "{\"age\":[{\"@type\":\"g:Int32\",\"@value\":29}],\"name\":[\"marko\"]}";
            Assert.Equal(expectedGraphSON, serializedDict);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeEdge(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var edge = new Edge(7, new Vertex(0, "person"), "knows", new Vertex(1, "dog"));

            var graphSON = writer.WriteObject(edge);

            const string expected =
                "{\"@type\":\"g:Edge\",\"@value\":{\"id\":{\"@type\":\"g:Int32\",\"@value\":7},\"outV\":{\"@type\":\"g:Int32\",\"@value\":0},\"outVLabel\":\"person\",\"label\":\"knows\",\"inV\":{\"@type\":\"g:Int32\",\"@value\":1},\"inVLabel\":\"dog\"}}";
            Assert.Equal(expected, graphSON);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeEnum(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var serializedEnum = writer.WriteObject(Direction.Both);

            var expectedGraphSON = "{\"@type\":\"g:Direction\",\"@value\":\"BOTH\"}";
            Assert.Equal(expectedGraphSON, serializedEnum);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeList(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var list = new List<int> {5, 6};

            var serializedGraphSON = writer.WriteObject(list.ToArray());

            var expectedGraphSON = "[{\"@type\":\"g:Int32\",\"@value\":5},{\"@type\":\"g:Int32\",\"@value\":6}]";
            Assert.Equal(expectedGraphSON, serializedGraphSON);
        }

        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldSerializeGList(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var list = new List<object> {5, 6, null};

            var serializedGraphSON = writer.WriteObject(list);

            var expectedGraphSON = "{\"@type\":\"g:List\",\"@value\":[{\"@type\":\"g:Int32\",\"@value\":5}," +
                                   "{\"@type\":\"g:Int32\",\"@value\":6},null]}";
            Assert.Equal(expectedGraphSON, serializedGraphSON);
        }

        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldSerializeGSet(int version)
        {
            var writer = CreateGraphSONWriter(version);
            ISet<object> set = new HashSet<object> {600L, 700L};

            var serializedGraphSON = writer.WriteObject(set);

            var expectedGraphSON = "{\"@type\":\"g:Set\",\"@value\":[{\"@type\":\"g:Int64\",\"@value\":600}," +
                                   "{\"@type\":\"g:Int64\",\"@value\":700}]}";
            Assert.Equal(expectedGraphSON, serializedGraphSON);
        }

        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldSerializeGMap(int version)
        {
            var writer = CreateGraphSONWriter(version);
            IDictionary<object, object> map = new Dictionary<object, object> { { 1L, "a"}, { 200L, "b"}};

            var serializedGraphSON = writer.WriteObject(map);

            var expectedGraphSON = "{\"@type\":\"g:Map\",\"@value\":[{\"@type\":\"g:Int64\",\"@value\":1},\"a\"," +
                                   "{\"@type\":\"g:Int64\",\"@value\":200},\"b\"]}";
            Assert.Equal(expectedGraphSON, serializedGraphSON);
        }
        
        [Theory, MemberData(nameof(VersionsNotSupportingCollections))]
        public void ShouldSerializePredicateWithMultipleValuesAsJSONArray(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var predicate = new P("within", new List<int> {1, 2});

            var serializedPredicate = writer.WriteObject(predicate);

            var expectedGraphSON =
                "{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"within\",\"value\":[{\"@type\":\"g:Int32\",\"@value\":1},{\"@type\":\"g:Int32\",\"@value\":2}]}}";
            Assert.Equal(expectedGraphSON, serializedPredicate);
            
            predicate = P.Within(1, 2);

            serializedPredicate = writer.WriteObject(predicate);

            Assert.Equal(expectedGraphSON, serializedPredicate);
        }

        [Theory, MemberData(nameof(VersionsSupportingCollections))]
        public void ShouldSerializePredicateWithMultipleValues(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var predicate = P.Within(new List<int> {1, 2});

            var serializedPredicate = writer.WriteObject(predicate);

            var expectedGraphSON =
                "{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"within\",\"value\":{\"@type\":\"g:List\",\"@value\":[{\"@type\":\"g:Int32\",\"@value\":1},{\"@type\":\"g:Int32\",\"@value\":2}]}}}";
            Assert.Equal(expectedGraphSON, serializedPredicate);

            predicate = P.Within(1, 2);

            serializedPredicate = writer.WriteObject(predicate);

            Assert.Equal(expectedGraphSON, serializedPredicate);

        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializePredicateWithSingleValue(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var predicate = new P("lt", 5);

            var serializedPredicate = writer.WriteObject(predicate);

            var expectedGraphSON =
                "{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"lt\",\"value\":{\"@type\":\"g:Int32\",\"@value\":5}}}";
            Assert.Equal(expectedGraphSON, serializedPredicate);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializePropertyWithEdgeElement(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var property = new Property("aKey", "aValue", new Edge("anId", new Vertex(1), "edgeLabel", new Vertex(2)));

            var graphSON = writer.WriteObject(property);

            const string expected =
                "{\"@type\":\"g:Property\",\"@value\":{\"key\":\"aKey\",\"value\":\"aValue\",\"element\":{\"@type\":\"g:Edge\",\"@value\":{\"id\":\"anId\",\"outV\":{\"@type\":\"g:Int32\",\"@value\":1},\"label\":\"edgeLabel\",\"inV\":{\"@type\":\"g:Int32\",\"@value\":2}}}}}";
            Assert.Equal(expected, graphSON);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializePropertyWithVertexPropertyElement(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var property = new Property("name", "marko",
                new VertexProperty("anId", "aKey", 21345, new Vertex("vertexId")));

            var graphSON = writer.WriteObject(property);

            const string expected =
                "{\"@type\":\"g:Property\",\"@value\":{\"key\":\"name\",\"value\":\"marko\",\"element\":{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":\"anId\",\"label\":\"aKey\",\"vertex\":\"vertexId\"}}}}";
            Assert.Equal(expected, graphSON);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeVertexProperty(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var vertexProperty = new VertexProperty("blah", "keyA", true, new Vertex("stephen"));

            var graphSON = writer.WriteObject(vertexProperty);

            const string expected =
                "{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":\"blah\",\"label\":\"keyA\",\"value\":true,\"vertex\":\"stephen\"}}";
            Assert.Equal(expected, graphSON);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeGuid(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var guid = Guid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786");

            var graphSon = writer.WriteObject(guid);

            const string expected = "{\"@type\":\"g:UUID\",\"@value\":\"41d2e28a-20a4-4ab0-b379-d810dede3786\"}";
            Assert.Equal(expected, graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeVertex(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var vertex = new Vertex(45.23f);

            var graphSON = writer.WriteObject(vertex);

            const string expected =
                "{\"@type\":\"g:Vertex\",\"@value\":{\"id\":{\"@type\":\"g:Float\",\"@value\":45.23},\"label\":\"vertex\"}}";
            Assert.Equal(expected, graphSON);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeVertexWithLabel(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var vertex = new Vertex((long) 123, "project");

            var graphSON = writer.WriteObject(vertex);

            const string expected =
                "{\"@type\":\"g:Vertex\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":123},\"label\":\"project\"}}";
            Assert.Equal(expected, graphSON);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeTypeToItsObject(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var type = typeof(SubgraphStrategy);

            var graphSon = writer.WriteObject(type);

            const string expected = "{\"@type\":\"g:SubgraphStrategy\",\"@value\":{}}";
            Assert.Equal(expected, graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeLambda(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var lambda = Lambda.Groovy("{ it.get() }");

            var graphSon = writer.WriteObject(lambda);

            const string expected =
                "{\"@type\":\"g:Lambda\",\"@value\":{\"script\":\"{ it.get() }\",\"language\":\"gremlin-groovy\",\"arguments\":-1}}";
            Assert.Equal(expected, graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeTimeSpan(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var timeSpan = new TimeSpan(5, 4, 3, 2, 1);

            var graphSon = writer.WriteObject(timeSpan);

            const string expected = "{\"@type\":\"gx:Duration\",\"@value\":\"P5DT4H3M2.001S\"}";
            Assert.Equal(expected, graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeBigInteger(int version)
        {
            var writer = CreateGraphSONWriter(version);
            var bigInteger = BigInteger.Parse("123456789987654321123456789987654321");

            var graphSon = writer.WriteObject(bigInteger);

            const string expected = "{\"@type\":\"gx:BigInteger\",\"@value\":\"123456789987654321123456789987654321\"}";
            Assert.Equal(expected, graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeByte(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject((byte)1);

            Assert.Equal("{\"@type\":\"gx:Byte\",\"@value\":1}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeByteBuffer(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject(Convert.FromBase64String("c29tZSBieXRlcyBmb3IgeW91"));

            Assert.Equal("{\"@type\":\"gx:ByteBuffer\",\"@value\":\"c29tZSBieXRlcyBmb3IgeW91\"}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeChar(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject('x');

            Assert.Equal("{\"@type\":\"gx:Char\",\"@value\":\"x\"}", graphSon);
        }

        [Theory, MemberData(nameof(Versions))]
        public void ShouldSerializeInt16(int version)
        {
            var writer = CreateGraphSONWriter(version);

            var graphSon = writer.WriteObject((short)100);

            Assert.Equal("{\"@type\":\"gx:Int16\",\"@value\":100}", graphSon);
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