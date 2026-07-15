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
using System.IO;
using System.Threading.Tasks;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    public class CompositePDTTests
    {
        private static readonly GraphBinaryWriter Writer = new();
        private static readonly GraphBinaryReader Reader = new();

        [Fact]
        public async Task TestRoundTripWithFields()
        {
            var fields = new Dictionary<string, object?> { { "x", 1 }, { "y", "hello" } };
            var expected = new CompositePDT("com.example.MyType", fields);

            using var stream = new MemoryStream();
            await Writer.WriteAsync(expected, stream);
            stream.Position = 0;
            var actual = await Reader.ReadAsync(stream) as CompositePDT;

            Assert.NotNull(actual);
            Assert.Equal(expected.Name, actual!.Name);
            Assert.Equal(expected.Fields, actual.Fields);
        }

        [Fact]
        public async Task TestRoundTripWithEmptyFields()
        {
            var expected = new CompositePDT("com.example.Empty", new Dictionary<string, object?>());

            using var stream = new MemoryStream();
            await Writer.WriteAsync(expected, stream);
            stream.Position = 0;
            var actual = await Reader.ReadAsync(stream) as CompositePDT;

            Assert.NotNull(actual);
            Assert.Equal(expected.Name, actual!.Name);
            Assert.Empty(actual.Fields);
        }

        [Fact]
        public async Task TestRoundTripWithNullFieldValue()
        {
            var fields = new Dictionary<string, object?> { { "key", null } };
            var expected = new CompositePDT("com.example.NullVal", fields);

            using var stream = new MemoryStream();
            await Writer.WriteAsync(expected, stream);
            stream.Position = 0;
            var actual = await Reader.ReadAsync(stream) as CompositePDT;

            Assert.NotNull(actual);
            Assert.Equal(expected.Name, actual!.Name);
            Assert.Null(actual.Fields["key"]);
        }

        [Fact]
        public async Task TestDataTypeCode()
        {
            var pdt = new CompositePDT("com.example.Test", new Dictionary<string, object?>());

            using var stream = new MemoryStream();
            await Writer.WriteAsync(pdt, stream);

            // First byte should be the CompositePDT type code 0xF0
            Assert.Equal(0xF0, stream.ToArray()[0]);
        }

        [Fact]
        public void TestConstructorThrowsOnNullName()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new CompositePDT(null!, new Dictionary<string, object?>()));
        }

        [Fact]
        public void TestConstructorThrowsOnEmptyName()
        {
            Assert.Throws<ArgumentException>(() =>
                new CompositePDT("", new Dictionary<string, object?>()));
        }

        [Fact]
        public void TestEquality()
        {
            var a = new CompositePDT("com.example.T", new Dictionary<string, object?> { { "k", 1 } });
            var b = new CompositePDT("com.example.T", new Dictionary<string, object?> { { "k", 1 } });
            Assert.Equal(a, b);
            Assert.Equal(a.GetHashCode(), b.GetHashCode());
        }

        [Fact]
        public void TestInequality()
        {
            var a = new CompositePDT("com.example.A", new Dictionary<string, object?>());
            var b = new CompositePDT("com.example.B", new Dictionary<string, object?>());
            Assert.NotEqual(a, b);
        }

        [Fact]
        public void TestToString()
        {
            var pdt = new CompositePDT("com.example.T", new Dictionary<string, object?> { { "x", 42 } });
            Assert.Contains("com.example.T", pdt.ToString());
            Assert.Contains("x=42", pdt.ToString());
        }
    }
}
