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
using System.IO;
using System.Threading.Tasks;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    public class PrimitivePDTTests
    {
        private static readonly GraphBinaryWriter Writer = new();
        private static readonly GraphBinaryReader Reader = new();

        [Fact]
        public async Task TestRoundTripBasic()
        {
            var expected = new PrimitivePDT("com.example.Uint32", "42");

            using var stream = new MemoryStream();
            await Writer.WriteAsync(expected, stream);
            stream.Position = 0;
            var actual = await Reader.ReadAsync(stream) as PrimitivePDT;

            Assert.NotNull(actual);
            Assert.Equal(expected.Name, actual!.Name);
            Assert.Equal(expected.Value, actual.Value);
        }

        [Fact]
        public async Task TestRoundTripWithLeadingZeros()
        {
            var expected = new PrimitivePDT("com.example.Padded", "007");

            using var stream = new MemoryStream();
            await Writer.WriteAsync(expected, stream);
            stream.Position = 0;
            var actual = await Reader.ReadAsync(stream) as PrimitivePDT;

            Assert.NotNull(actual);
            Assert.Equal("007", actual!.Value);
        }

        [Fact]
        public async Task TestRoundTripWithLargeNumber()
        {
            var expected = new PrimitivePDT("com.example.BigNum",
                "99999999999999999999999999999999");

            using var stream = new MemoryStream();
            await Writer.WriteAsync(expected, stream);
            stream.Position = 0;
            var actual = await Reader.ReadAsync(stream) as PrimitivePDT;

            Assert.NotNull(actual);
            Assert.Equal("99999999999999999999999999999999", actual!.Value);
        }

        [Fact]
        public async Task TestRoundTripNonNumericValue()
        {
            var expected = new PrimitivePDT("com.example.Token", "abc-def-123");

            using var stream = new MemoryStream();
            await Writer.WriteAsync(expected, stream);
            stream.Position = 0;
            var actual = await Reader.ReadAsync(stream) as PrimitivePDT;

            Assert.NotNull(actual);
            Assert.Equal("abc-def-123", actual!.Value);
        }

        [Fact]
        public async Task TestRoundTripEmptyValue()
        {
            var expected = new PrimitivePDT("com.example.Empty", "");

            using var stream = new MemoryStream();
            await Writer.WriteAsync(expected, stream);
            stream.Position = 0;
            var actual = await Reader.ReadAsync(stream) as PrimitivePDT;

            Assert.NotNull(actual);
            Assert.Equal("", actual!.Value);
        }

        [Fact]
        public async Task TestDataTypeCode()
        {
            var pdt = new PrimitivePDT("com.example.Test", "val");

            using var stream = new MemoryStream();
            await Writer.WriteAsync(pdt, stream);

            Assert.Equal(0xF1, stream.ToArray()[0]);
        }

        [Fact]
        public void TestConstructorThrowsOnNullName()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new PrimitivePDT(null!, "val"));
        }

        [Fact]
        public void TestConstructorThrowsOnEmptyName()
        {
            Assert.Throws<ArgumentException>(() =>
                new PrimitivePDT("", "val"));
        }

        [Fact]
        public void TestConstructorThrowsOnNullValue()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new PrimitivePDT("com.example.T", null!));
        }

        [Fact]
        public void TestEquality()
        {
            var a = new PrimitivePDT("com.example.T", "42");
            var b = new PrimitivePDT("com.example.T", "42");
            Assert.Equal(a, b);
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        [Fact]
        public void TestInequality()
        {
            var a = new PrimitivePDT("com.example.A", "1");
            var b = new PrimitivePDT("com.example.B", "1");
            Assert.NotEqual(a, b);
        }

        [Fact]
        public void TestToString()
        {
            var pdt = new PrimitivePDT("com.example.T", "42");
            Assert.Contains("com.example.T", pdt.ToString());
            Assert.Contains("42", pdt.ToString());
        }

        [Fact]
        public async Task TestHydrationWithRegistry()
        {
            var registry = new PDTRegistry();
            registry.RegisterPrimitive(new TestUint32Adapter());
            var reader = new GraphBinaryReader(pdtRegistry: registry);

            var pdt = new PrimitivePDT("test:Uint32", "123");
            using var stream = new MemoryStream();
            await Writer.WriteAsync(pdt, stream);
            stream.Position = 0;
            var result = await reader.ReadAsync(stream);

            Assert.IsType<uint>(result);
            Assert.Equal(123u, (uint)result);
        }

        [Fact]
        public async Task TestNoHydrationWithoutRegistry()
        {
            var pdt = new PrimitivePDT("test:Uint32", "456");
            using var stream = new MemoryStream();
            await Writer.WriteAsync(pdt, stream);
            stream.Position = 0;
            var result = await Reader.ReadAsync(stream);

            Assert.IsType<PrimitivePDT>(result);
            Assert.Equal("456", ((PrimitivePDT)result).Value);
        }

        private class TestUint32Adapter : IPrimitivePdtAdapter<uint>
        {
            public string TypeName => "test:Uint32";
            public uint FromString(string value) => uint.Parse(value);
            public string ToString(uint obj) => obj.ToString();
        }
    }
}
