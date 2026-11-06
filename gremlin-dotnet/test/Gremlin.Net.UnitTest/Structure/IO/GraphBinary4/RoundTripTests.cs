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
using System.Linq;
using System.Threading.Tasks;
using Gremlin.Net.Structure.IO.GraphBinary4;
using GremlinEdge = Gremlin.Net.Structure.Edge;
using GremlinGraph = Gremlin.Net.Structure.Graph;
using GremlinPath = Gremlin.Net.Structure.Path;
using GremlinProperty = Gremlin.Net.Structure.Property;
using GremlinVertex = Gremlin.Net.Structure.Vertex;
using GremlinVertexProperty = Gremlin.Net.Structure.VertexProperty;
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

        private static bool OrderedMapComparator(object? x, object? y)
        {
            if (x is not IDictionary<object, object?> expected || y is not IDictionary<object, object?> actual)
                return false;
            if (expected.Count != actual.Count)
                return false;

            return expected.Zip(actual).All(pair =>
                Equals(pair.First.Key, pair.Second.Key) && Equals(pair.First.Value, pair.Second.Value));
        }

        private static bool PathComparator(object? x, object? y)
        {
            if (x is not GremlinPath expected || y is not GremlinPath actual)
                return false;
            if (!expected.Objects.SequenceEqual(actual.Objects) || expected.Labels.Count != actual.Labels.Count)
                return false;

            for (var ix = 0; ix < expected.Labels.Count; ix++)
            {
                if (!expected.Labels[ix].SetEquals(actual.Labels[ix]))
                    return false;
            }

            return true;
        }

        private static bool VertexLabelComparator(object? x, object? y)
        {
            if (x is not GremlinVertex expected || y is not GremlinVertex actual)
                return false;

            return expected.Equals(actual) &&
                   expected.Labels.Count == actual.Labels.Count &&
                   expected.Labels.All(actual.Labels.Contains);
        }

        private static bool GraphComparator(object? x, object? y)
        {
            if (x is not GremlinGraph expected || y is not GremlinGraph actual)
                return false;
            if (expected.Vertices.Count != actual.Vertices.Count || expected.Edges.Count != actual.Edges.Count)
                return false;

            foreach (var pair in expected.Vertices)
            {
                if (!actual.Vertices.TryGetValue(pair.Key, out var actualVertex) ||
                    !VertexComparator(pair.Value, actualVertex))
                    return false;
            }

            foreach (var pair in expected.Edges)
            {
                if (!actual.Edges.TryGetValue(pair.Key, out var actualEdge) || !EdgeComparator(pair.Value, actualEdge))
                    return false;
            }

            return true;
        }

        private static bool VertexComparator(GremlinVertex expected, GremlinVertex actual)
        {
            return expected.Equals(actual) &&
                   expected.Label == actual.Label &&
                   LabelsEqual(expected.Labels, actual.Labels) &&
                   VertexPropertiesEqual(expected.Properties, actual.Properties);
        }

        private static bool EdgeComparator(GremlinEdge expected, GremlinEdge actual)
        {
            return expected.Equals(actual) &&
                   expected.Label == actual.Label &&
                   LabelsEqual(expected.Labels, actual.Labels) &&
                   Equals(expected.OutV.Id, actual.OutV.Id) &&
                   Equals(expected.InV.Id, actual.InV.Id) &&
                   PropertiesEqual(expected.Properties, actual.Properties);
        }

        private static bool VertexPropertiesEqual(dynamic[] expected, dynamic[] actual)
        {
            if (expected.Length != actual.Length)
                return false;

            for (var ix = 0; ix < expected.Length; ix++)
            {
                if (expected[ix] is not GremlinVertexProperty expectedProperty ||
                    actual[ix] is not GremlinVertexProperty actualProperty ||
                    !VertexPropertyComparator(expectedProperty, actualProperty))
                    return false;
            }

            return true;
        }

        private static bool VertexPropertyComparator(GremlinVertexProperty expected, GremlinVertexProperty actual)
        {
            return expected.Equals(actual) &&
                   expected.Label == actual.Label &&
                   Equals(expected.Value, actual.Value) &&
                   PropertiesEqual(expected.Properties, actual.Properties);
        }

        private static bool PropertiesEqual(dynamic[] expected, dynamic[] actual)
        {
            if (expected.Length != actual.Length)
                return false;

            for (var ix = 0; ix < expected.Length; ix++)
            {
                if (expected[ix] is not GremlinProperty expectedProperty ||
                    actual[ix] is not GremlinProperty actualProperty ||
                    !PropertyComparator(expectedProperty, actualProperty))
                    return false;
            }

            return true;
        }

        private static bool PropertyComparator(GremlinProperty expected, GremlinProperty actual)
        {
            return expected.Key == actual.Key && Equals(expected.Value, actual.Value);
        }

        private static bool LabelsEqual(IReadOnlySet<string> expected, IReadOnlySet<string> actual)
        {
            return expected.Count == actual.Count && expected.All(actual.Contains);
        }

        // BigInteger tests
        [Fact]
        public Task TestPosBigInteger() => Run("pos-biginteger");

        [Fact]
        public Task TestNegBigInteger() => Run("neg-biginteger");

        [Fact]
        public Task TestZeroBigInteger() => Run("zero-biginteger");

        [Fact]
        public Task TestSignBoundaryPosBigInteger() => Run("sign-boundary-pos-biginteger");

        [Fact]
        public Task TestSignBoundaryNegBigInteger() => Run("sign-boundary-neg-biginteger");

        // BigDecimal tests
        [Fact]
        public Task TestZeroBigDecimal() => Run("zero-bigdecimal");

        [Fact]
        public Task TestScaleZeroBigDecimal() => Run("scale-zero-bigdecimal");

        [Fact]
        public Task TestNegativeScaleBigDecimal() =>
            RunWriteRead("negative-scale-bigdecimal"); // decimal doesn't preserve negative scale for byte-exact writes

        [Fact]
        public Task TestSmallDecimalBigDecimal() => Run("small-decimal-bigdecimal");

        // Provider-defined type tests
        [Fact]
        public Task TestUint8PrimitivePdt() => Run("uint8-primitive-pdt");

        [Fact]
        public Task TestPointCompositePdt() => Run("point-composite-pdt");

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
        public Task TestTwoByteChar() => Run("two-byte-char");

        [Fact]
        public Task TestThreeByteChar() => Run("three-byte-char");

        // Null test
        [Fact]
        public Task TestUnspecifiedNull() => Run("unspecified-null");

        [Fact]
        public Task TestNullInt() => RunRead("null-int"); // typed nulls deserialize to plain null

        [Fact]
        public Task TestNullLong() => RunRead("null-long"); // typed nulls deserialize to plain null

        [Fact]
        public Task TestNullString() => RunRead("null-string"); // typed nulls deserialize to plain null

        [Fact]
        public Task TestNullList() => RunRead("null-list"); // typed nulls deserialize to plain null

        [Fact]
        public Task TestNullMap() => RunRead("null-map"); // typed nulls deserialize to plain null

        [Fact]
        public Task TestNullSet() => RunRead("null-set"); // typed nulls deserialize to plain null

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

        [Fact]
        public Task TestEmptyString() => Run("empty-string");

        // BulkSet tests (read only - BulkSet deserialized as List)
        [Fact]
        public Task TestVarBulkList() => RunRead("var-bulklist"); // BulkList deserializes as a plain List

        [Fact]
        public Task TestEmptyBulkList() => RunRead("empty-bulklist"); // BulkList deserializes as a plain List

        // Duration tests
        [Fact]
        public Task TestZeroDuration() => Run("zero-duration");

        [Fact]
        public Task TestPositiveDuration() => Run("positive-duration");

        [Fact]
        public Task TestNegativeDuration() => Run("negative-duration");

        [Fact]
        public Task TestNanosDuration() =>
            RunWriteRead("nanos-duration"); // TimeSpan truncates sub-tick nanoseconds for byte-exact writes

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

        [Fact]
        public Task TestOrderedStringIntMap() =>
            RunWriteRead("ordered-string-int-map", OrderedMapComparator); // writer doesn't emit the ordered-map value flag

        // Path tests
        [Fact]
        public Task TestTraversalPath() => RunWriteRead("traversal-path"); // properties written as null not empty list

        [Fact]
        public Task TestEmptyPath() => Run("empty-path");

        [Fact]
        public Task TestPathZeroLabels() => Run("path-zero-labels", PathComparator);

        [Fact]
        public Task TestPathMultipleLabels() =>
            RunWriteRead("path-multiple-labels", PathComparator); // label set ordering isn't guaranteed

        [Fact]
        public Task TestPropPath() => RunWriteRead("prop-path"); // properties aren't serialized

        // Tree tests
        [Fact]
        public Task TestTraversalTree() => RunWriteRead("traversal-tree"); // vertex properties aren't serialized

        [Fact]
        public Task TestEmptyTree() => Run("empty-tree");

        [Fact]
        public Task TestTreeNullKey() => Run("tree-null-key");

        [Fact]
        public Task TestTreeMixedKeyTypes() => Run("tree-mixed-key-types");

        [Fact]
        public Task TestTreeDeepNesting() => Run("tree-deep-nesting");

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

        [Fact]
        public Task TestTinkerGraph() => Run("tinker-graph", GraphComparator);

        [Fact]
        public Task TestMultiLabelVertex() =>
            RunWriteRead("multi-label-vertex", VertexLabelComparator); // Vertex equality is id-only

        [Fact]
        public Task TestEmptyLabelVertex() =>
            RunWriteRead("empty-label-vertex", VertexLabelComparator); // Vertex equality is id-only

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

        // Merge enum tests
        [Fact]
        public Task TestMergeOnCreate() => Run("merge-on-create");

        [Fact]
        public Task TestMergeOnMatch() => Run("merge-on-match");

        [Fact]
        public Task TestMergeOutV() => Run("merge-out-v");

        [Fact]
        public Task TestMergeInV() => Run("merge-in-v");
    }
}
