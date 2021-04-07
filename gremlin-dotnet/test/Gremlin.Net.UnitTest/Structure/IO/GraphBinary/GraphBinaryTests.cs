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
using System.Globalization;
using System.IO;
using System.Numerics;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary;
using Xunit;
using Path = Gremlin.Net.Structure.Path;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary
{
    public class GraphBinaryTests
    {
        [Fact]
        public async Task TestNull()
        {
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(null, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Null(actual);
        }
    
        [Fact]
        public async Task TestInt()
        {
            const int expected = 100;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }

        [Theory]
        [InlineData(1, new byte[]{0x00, 0x00, 0x00, 0x01})]
        [InlineData(257, new byte[]{0x00, 0x00, 0x01, 0x01})]
        [InlineData(-1, new byte[]{0xFF, 0xFF, 0xFF, 0xFF})]
        [InlineData(-2, new byte[]{0xFF, 0xFF, 0xFF, 0xFE})]
        public async Task TestIntSpec(int value, byte[] expected)
        {
            var writer = CreateGraphBinaryWriter();
            var serializationStream = new MemoryStream();
            
            await writer.WriteValueAsync(value, serializationStream, false);

            var serBytes = serializationStream.ToArray();
            Assert.Equal(expected, serBytes);
        }
        
        [Fact]
        public async Task TestLong()
        {
            const long expected = 100;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Theory]
        [InlineData(100.01f)]
        [InlineData(float.NaN)]
        [InlineData(float.NegativeInfinity)]
        [InlineData(float.PositiveInfinity)]
        public async Task TestFloat(float expected)
        {
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestDouble()
        {
            const double expected = 100.001;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestShort()
        {
            const short expected = 100;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestDate()
        {
            var expected = DateTimeOffset.ParseExact("2016-12-14 16:14:36.295000", "yyyy-MM-dd HH:mm:ss.ffffff",
                CultureInfo.InvariantCulture);
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        // TODO: Test timestamp, problem: same C# as for date

        [Fact]
        public async Task TestString()
        {
            const string expected = "serialize this!";
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Theory]
        [InlineData("serialize this!", "serialize that!", "serialize that!", "stop telling me what to serialize")]
        [InlineData(1, 2, 3, 4, 5)]
        [InlineData(0.1, 1.1, 2.5, double.NaN)]
        [InlineData(0.1f, 1.1f, 2.5f, float.NaN)]
        public async Task TestHomogeneousList(params object[] listMembers)
        {
            var expected = new List<object>(listMembers);
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestHomogeneousTypeSafeList()
        {
            var expected = new List<string> {"test", "123"};
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestHeterogeneousList()
        {
            var expected = new List<object>
                {"serialize this!", 0, "serialize that!", "serialize that!", 1, "stop telling me what to serialize", 2};
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task TestArray()
        {
            var expected = new string[] {"hallo", "welt"};
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Theory]
        [InlineData("serialize this!", "serialize that!", "serialize that!", "stop telling me what to serialize")]
        [InlineData(1, 2, 3, 4, 5)]
        [InlineData(0.1, 1.1, 2.5, double.NaN)]
        [InlineData(0.1f, 1.1f, 2.5f, float.NaN)]
        public async Task TestHomogeneousSet(params object[] listMembers)
        {
            var expected = new HashSet<object>(listMembers);
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestHomogeneousTypeSafeSet()
        {
            var expected = new HashSet<string> {"test", "123"};
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteValueAsync(expected, serializationStream, false);
            serializationStream.Position = 0;
            var actual = await reader.ReadValueAsync<HashSet<string>>(serializationStream, false);
            
            Assert.Equal(expected, actual);
            Assert.Equal(expected.GetType(), actual.GetType());
        }
        
        [Fact]
        public async Task TestHeterogeneousSet()
        {
            var expected = new HashSet<object>
                {"serialize this!", 0, "serialize that!", "serialize that!", 1, "stop telling me what to serialize", 2};
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task TestDictionary()
        {
            var expected = new Dictionary<object, object>
            {
                {"yo", "what"},
                {"go", "no!"},
                {"number", 123},
                {321, "crazy with the number for a key"},
                {987, new List<object> {"go", "deep", new Dictionary<object, object> {{"here", "!"}}}}
            };
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestHomogeneousTypeSafeDictionary()
        {
            var expected = new Dictionary<string, int>
            {
                {"number", 123},
                {"and", 456},
                {"nothing else", 789}
            };
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestHomogeneousTypeSafeDictionaryWithCorrectTyping()
        {
            var expected = new Dictionary<string, int>
            {
                {"number", 123},
                {"and", 456},
                {"nothing else", 789}
            };
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteValueAsync(expected, serializationStream, false);
            serializationStream.Position = 0;
            var actual = await reader.ReadValueAsync<Dictionary<string, int>>(serializationStream, false);
            
            Assert.Equal(expected, actual);
            Assert.Equal(expected.GetType(), actual.GetType());
        }
        
        [Fact]
        public async Task TestGuid()
        {
            var expected = Guid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786");
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task TestGuidSerialization()
        {
            var toSerialize = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
            var writer = CreateGraphBinaryWriter();
            var serializationStream = new MemoryStream();

            await writer.WriteValueAsync(toSerialize, serializationStream, false);

            var expected = new byte[]
                {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
            Assert.Equal(expected, serializationStream.ToArray());
        }

        [Fact]
        public async Task TestVertex()
        {
            var expected = new Vertex(123, "person");
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestVertexWithNullLabel()
        {
            var expected = new Vertex(123, null);
            var writer = CreateGraphBinaryWriter();
            var serializationStream = new MemoryStream();
            
            await Assert.ThrowsAsync<IOException>(() => writer.WriteAsync(expected, serializationStream));
        }
        
        [Fact]
        public async Task TestEdge()
        {
            var expected = new Edge(123, new Vertex(1, "person"), "developed", new Vertex(10, "software"));
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task TestPath()
        {
            var expected =
                new Path(
                    new List<ISet<string>>
                        {new HashSet<string> {"a", "b"}, new HashSet<string> {"c", "d"}, new HashSet<string> {"e"}},
                    new List<object> {1, 2, 3});
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task TestProperty()
        {
            var expected = new Property("name", "stephen", null);
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestVertexProperty()
        {
            var expected = new VertexProperty(123, "name", "stephen", null);
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestBarrier()
        {
            var expected = Barrier.NormSack;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestCardinality()
        {
            var expected = Cardinality.Set;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestColumn()
        {
            var expected = Column.Values;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestDirection()
        {
            var expected = Direction.Out;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestOperator()
        {
            var expected = Operator.Min;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestOrder()
        {
            var expected = Order.Shuffle;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestPick()
        {
            var expected = Pick.None;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestPop()
        {
            var expected = Pop.All;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestScope()
        {
            var expected = Scope.Local;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestT()
        {
            var expected = T.Label;
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestBinding()
        {
            var expected = new Binding("name", "marko");
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestBytecode()
        {
            var expected = new Bytecode();
            expected.SourceInstructions.Add(new Instruction("withStrategies", "SubgraphStrategy"));
            expected.StepInstructions.Add(new Instruction("V", 1, 2, 3));
            expected.StepInstructions.Add(new Instruction("out"));
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = (Bytecode) await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected.SourceInstructions, actual.SourceInstructions);
            Assert.Equal(expected.StepInstructions, actual.StepInstructions);
        }
        
        [Theory]
        [InlineData(1)]
        [InlineData(123)]
        public async Task TestByte(byte expected)
        {
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestByteBuffer()
        {
            var expected = new byte[] {1, 2, 3};
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestBoolean(bool expected)
        {
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Theory]
        [InlineData('a')]
        [InlineData('0')]
        [InlineData('¢')]
        [InlineData('€')]
        public async Task TestChar(char expected)
        {
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestDuration()
        {
            var expected = new TimeSpan(1, 2, 3, 4, 5);
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestBigInteger()
        {
            var expected = BigInteger.Parse("123456789987654321123456789987654321");
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Theory]
        [InlineData("190.035")]
        [InlineData("0.19")]
        [InlineData("1900")]
        [InlineData("-1900")]
        [InlineData("100000000000000")]
        [InlineData("100000000000000000000000000")]
        public async Task TestBigDecimal(string decimalValue)
        {
            var expected = Decimal.Parse(decimalValue);
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task TestLambda()
        {
            var expected = Lambda.Groovy("some expression");
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = (StringBasedLambda) await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected.Language, actual.Language);
            Assert.Equal(expected.LambdaExpression, actual.LambdaExpression);
            Assert.Equal(expected.Arguments, actual.Arguments);
        }
        
        [Fact]
        public async Task TestPBetween()
        {
            var expected = P.Between(1, 2);
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = (P) await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected.OperatorName, actual.OperatorName);
            Assert.Equal(expected.Other, actual.Other);
            Assert.Equal(expected.Value, actual.Value);
        }
        
        [Fact]
        public async Task TestPGt()
        {
            var expected = P.Gt(5);
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = (P) await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected.OperatorName, actual.OperatorName);
            Assert.Equal(expected.Other, actual.Other);
            Assert.Equal(expected.Value, actual.Value);
        }

        [Fact]
        public async Task TestPAnd()
        {
            var expected = P.Not(P.Lte(10).And(P.Not(P.Between(11, 20)))).And(P.Lt(29).Or(P.Eq(35)));
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = (P) await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected.ToString(), actual.ToString());
        }
        
        [Fact]
        public async Task TestTextP()
        {
            var expected = TextP.Containing("o");
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = (TextP) await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected.OperatorName, actual.OperatorName);
            Assert.Equal(expected.Other, actual.Other);
            Assert.Equal(expected.Value, actual.Value);
        }
        
        [Fact]
        public async Task TestTraverser()
        {
            var expected = new Traverser("test", 3);
            var writer = CreateGraphBinaryWriter();
            var reader = CreateGraphBinaryReader();
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = (Traverser) await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected.Object, actual.Object);
            Assert.Equal(expected.Bulk, actual.Bulk);
        }
        
        private static GraphBinaryWriter CreateGraphBinaryWriter()
        {
            return new GraphBinaryWriter();
        }
        
        private static GraphBinaryReader CreateGraphBinaryReader()
        {
            return new GraphBinaryReader();
        }
    }
}