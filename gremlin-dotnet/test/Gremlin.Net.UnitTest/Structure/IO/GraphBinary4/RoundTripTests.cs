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
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    /// <summary>
    /// Round trip testing of GraphBinary 4.0 compared to a correct "model".
    /// Set the IO_TEST_DIRECTORY environment variable to the directory where
    /// the .gbin files that represent the serialized "model" are located.
    /// </summary>
    public class RoundTripTests
    {
        private const string GremlinTestDir = "gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/structure/io/graphbinary/";
        private const string DirectorySearchPattern = "gremlin-dotnet";
        
        private static readonly string TestResourceDirectory;
        private static readonly GraphBinaryWriter Writer = new();
        private static readonly GraphBinaryReader Reader = new();

        static RoundTripTests()
        {
            var envDir = Environment.GetEnvironmentVariable("IO_TEST_DIRECTORY");
            if (!string.IsNullOrEmpty(envDir))
            {
                TestResourceDirectory = envDir;
            }
            else
            {
                // Find the root directory by searching for gremlin-dotnet in the current path
                var currentDir = AppDomain.CurrentDomain.BaseDirectory;
                var index = currentDir.IndexOf(DirectorySearchPattern, StringComparison.Ordinal);
                var defaultDir = index >= 0 ? currentDir[..index] : currentDir;
                TestResourceDirectory = Path.Combine(defaultDir, GremlinTestDir);
            }
        }

        private static object? GetEntry(string title) => Model.Entries[title];

        private static byte[] ReadFileByName(string resourceName)
        {
            var fullName = Path.Combine(TestResourceDirectory, $"{resourceName}-v4.gbin");
            return File.ReadAllBytes(fullName);
        }

        /// <summary>
        /// Runs the regular set of tests for the type:
        /// 1. model to deserialized object
        /// 2. written bytes to read bytes
        /// 3. round tripped (read then written) bytes to read bytes
        /// </summary>
        private async Task Run(string resourceName, Func<object?, object?, bool>? comparator = null)
        {
            var resourceBytes = ReadFileByName(resourceName);
            var model = GetEntry(resourceName);
            
            using var readStream = new MemoryStream(resourceBytes);
            var read = await Reader.ReadAsync(readStream);
            
            if (comparator != null)
                Assert.True(comparator(model, read));
            else
                Assert.Equal(model, read);
            
            using var writeStream = new MemoryStream();
            await Writer.WriteAsync(model, writeStream);
            Assert.Equal(resourceBytes, writeStream.ToArray());
            
            using var roundTripReadStream = new MemoryStream(resourceBytes);
            var roundTripRead = await Reader.ReadAsync(roundTripReadStream);
            using var roundTripWriteStream = new MemoryStream();
            await Writer.WriteAsync(roundTripRead, roundTripWriteStream);
            Assert.Equal(resourceBytes, roundTripWriteStream.ToArray());
        }

        /// <summary>
        /// Runs the read test which compares the model to deserialized object.
        /// This should only be used in cases where there is only a deserializer
        /// but no serializer for the same type.
        /// </summary>
        private async Task RunRead(string resourceName, Func<object?, object?, bool>? comparator = null)
        {
            var resourceBytes = ReadFileByName(resourceName);
            var model = GetEntry(resourceName);
            
            using var readStream = new MemoryStream(resourceBytes);
            var read = await Reader.ReadAsync(readStream);
            
            if (comparator != null)
                Assert.True(comparator(model, read));
            else
                Assert.Equal(model, read);
        }

        /// <summary>
        /// Runs a reduced set of tests for the type:
        /// 1. model to deserialized object
        /// 2. model to round tripped (written then read) object
        /// Use this in cases where the regular Run() function is too stringent.
        /// E.g. when ordering doesn't matter like for sets.
        /// </summary>
        private async Task RunWriteRead(string resourceName, Func<object?, object?, bool>? comparator = null)
        {
            var resourceBytes = ReadFileByName(resourceName);
            var model = GetEntry(resourceName);
            
            using var readStream = new MemoryStream(resourceBytes);
            var read = await Reader.ReadAsync(readStream);
            
            using var writeStream = new MemoryStream();
            await Writer.WriteAsync(model, writeStream);
            using var roundTripStream = new MemoryStream(writeStream.ToArray());
            var roundTripped = await Reader.ReadAsync(roundTripStream);
            
            if (comparator != null)
            {
                Assert.True(comparator(model, read));
                Assert.True(comparator(model, roundTripped));
            }
            else
            {
                Assert.Equal(model, read);
                Assert.Equal(model, roundTripped);
            }
        }

        private static bool NanComparator(object? x, object? y)
        {
            return x switch
            {
                double dx when y is double dy => double.IsNaN(dx) && double.IsNaN(dy),
                float fx when y is float fy => float.IsNaN(fx) && float.IsNaN(fy),
                _ => false
            };
        }

        // BigInteger tests
        [Fact]
        public Task TestPosBigInteger() => Run("pos-biginteger");

        [Fact]
        public Task TestNegBigInteger() => Run("neg-biginteger");

        // Byte tests
        [Fact]
        public Task TestMinByte() => Run("min-byte");

        [Fact]
        public Task TestMaxByte() => Run("max-byte");

        // Binary tests
        [Fact]
        public Task TestEmptyBinary() => Run("empty-binary");

        [Fact]
        public Task TestStrBinary() => Run("str-binary");

        // Double tests
        [Fact]
        public Task TestMaxDouble() => Run("max-double");

        [Fact]
        public Task TestMinDouble() => Run("min-double");

        [Fact]
        public Task TestNegMaxDouble() => Run("neg-max-double");

        [Fact]
        public Task TestNegMinDouble() => Run("neg-min-double");

        [Fact]
        public Task TestNanDouble() => RunWriteRead("nan-double", NanComparator); // C# has different NaN representation

        [Fact]
        public Task TestPosInfDouble() => Run("pos-inf-double");

        [Fact]
        public Task TestNegInfDouble() => Run("neg-inf-double");

        [Fact]
        public Task TestNegZeroDouble() => Run("neg-zero-double");

        // Float tests
        [Fact]
        public Task TestMaxFloat() => Run("max-float");

        [Fact]
        public Task TestMinFloat() => Run("min-float");

        [Fact]
        public Task TestNegMaxFloat() => Run("neg-max-float");

        [Fact]
        public Task TestNegMinFloat() => Run("neg-min-float");

        [Fact]
        public Task TestNanFloat() => RunWriteRead("nan-float", NanComparator); // C# has different NaN representation

        [Fact]
        public Task TestPosInfFloat() => Run("pos-inf-float");

        [Fact]
        public Task TestNegInfFloat() => Run("neg-inf-float");

        [Fact]
        public Task TestNegZeroFloat() => Run("neg-zero-float");

        // Char tests
        [Fact]
        public Task TestSingleByteChar() => Run("single-byte-char");

        [Fact]
        public Task TestMultiByteChar() => Run("multi-byte-char");

        // Null test
        [Fact]
        public Task TestUnspecifiedNull() => Run("unspecified-null");

        // Boolean tests
        [Fact]
        public Task TestTrueBoolean() => Run("true-boolean");

        [Fact]
        public Task TestFalseBoolean() => Run("false-boolean");

        // String tests
        [Fact]
        public Task TestSingleByteString() => Run("single-byte-string");

        [Fact]
        public Task TestMixedString() => Run("mixed-string");

        // BulkSet tests (read only - BulkSet deserialized as List)
        [Fact]
        public Task TestVarBulkList() => RunRead("var-bulklist");

        [Fact]
        public Task TestEmptyBulkList() => RunRead("empty-bulklist");

        // Duration tests
        [Fact]
        public Task TestZeroDuration() => Run("zero-duration");

        // Edge tests
        [Fact]
        public Task TestTraversalEdge() => RunWriteRead("traversal-edge"); // properties aren't serialized

        [Fact]
        public Task TestNoPropEdge() => RunWriteRead("no-prop-edge"); // properties written as null not empty list

        // Int tests
        [Fact]
        public Task TestMaxInt() => Run("max-int");

        [Fact]
        public Task TestMinInt() => Run("min-int");

        // Long tests
        [Fact]
        public Task TestMaxLong() => Run("max-long");

        [Fact]
        public Task TestMinLong() => Run("min-long");

        // List tests
        [Fact]
        public Task TestVarTypeList() => Run("var-type-list");

        [Fact]
        public Task TestEmptyList() => Run("empty-list");

        // Map tests
        [Fact]
        public Task TestEmptyMap() => Run("empty-map");

        // Path tests
        [Fact]
        public Task TestTraversalPath() => RunWriteRead("traversal-path"); // properties written as null not empty list

        [Fact]
        public Task TestEmptyPath() => Run("empty-path");

        [Fact]
        public Task TestPropPath() => RunWriteRead("prop-path"); // properties aren't serialized

        // Property tests
        [Fact]
        public Task TestEdgeProperty() => Run("edge-property");

        [Fact]
        public Task TestNullProperty() => Run("null-property");

        // Set tests
        [Fact]
        public Task TestVarTypeSet() => RunWriteRead("var-type-set"); // order not guaranteed in HashSet

        [Fact]
        public Task TestEmptySet() => Run("empty-set");

        // Short tests
        [Fact]
        public Task TestMaxShort() => Run("max-short");

        [Fact]
        public Task TestMinShort() => Run("min-short");

        // UUID tests
        [Fact]
        public Task TestSpecifiedUuid() => Run("specified-uuid");

        [Fact]
        public Task TestNilUuid() => Run("nil-uuid");

        // Vertex tests
        [Fact]
        public Task TestNoPropVertex() => RunWriteRead("no-prop-vertex"); // properties written as null not empty list

        [Fact]
        public Task TestTraversalVertex() => RunWriteRead("traversal-vertex"); // properties aren't serialized

        // VertexProperty tests
        [Fact]
        public Task TestTraversalVertexProperty() => RunWriteRead("traversal-vertexproperty"); // properties aren't serialized

        [Fact]
        public Task TestMetaVertexProperty() => RunWriteRead("meta-vertexproperty"); // properties aren't serialized

        [Fact]
        public Task TestSetCardinalityVertexProperty() => RunWriteRead("set-cardinality-vertexproperty"); // properties aren't serialized

        // T enum test
        [Fact]
        public Task TestIdT() => Run("id-t");

        // Direction enum test
        [Fact]
        public Task TestOutDirection() => Run("out-direction");
    }
}
