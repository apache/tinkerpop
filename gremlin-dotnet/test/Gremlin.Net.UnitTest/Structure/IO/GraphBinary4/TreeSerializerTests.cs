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

using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    public class TreeSerializerTests
    {
        [Fact]
        public async Task ShouldRoundTripNestedTree()
        {
            // marko -> {lop, josh -> {ripple, lop}}
            var tree = new Tree();
            var marko = tree.GetOrCreateChild("marko");
            marko.GetOrCreateChild("lop");
            var josh = marko.GetOrCreateChild("josh");
            josh.GetOrCreateChild("ripple");
            josh.GetOrCreateChild("lop");

            var deserialized = await RoundTrip(tree);

            Assert.Equal(tree, deserialized);
            Assert.True(deserialized.HasChild("marko"));
            var rMarko = deserialized.ChildAt("marko");
            Assert.True(rMarko.HasChild("lop"));
            Assert.True(rMarko.HasChild("josh"));
            var rJosh = rMarko.ChildAt("josh");
            Assert.True(rJosh.HasChild("ripple"));
            Assert.True(rJosh.HasChild("lop"));
            Assert.True(rJosh.ChildAt("ripple").IsLeaf());
        }

        [Fact]
        public async Task ShouldRoundTripEmptyTree()
        {
            var tree = new Tree();

            var deserialized = await RoundTrip(tree);

            Assert.True(deserialized.IsLeaf());
            Assert.Equal(tree, deserialized);
        }

        [Fact]
        public async Task ShouldRoundTripTreeWithNullKey()
        {
            var tree = new Tree();
            tree.GetOrCreateChild(null).GetOrCreateChild("child");

            var deserialized = await RoundTrip(tree);

            Assert.True(deserialized.HasChild(null));
            Assert.True(deserialized.ChildAt(null).HasChild("child"));
            Assert.Equal(tree, deserialized);
        }

        [Fact]
        public void PrettyPrintShouldMatchCanonicalStyle()
        {
            var tree = new Tree();
            var marko = tree.GetOrCreateChild("marko");
            marko.GetOrCreateChild("lop");
            var josh = marko.GetOrCreateChild("josh");
            josh.GetOrCreateChild("ripple");

            var expected = "|--marko\n   |--lop\n   |--josh\n      |--ripple";
            Assert.Equal(expected, tree.PrettyPrint());
        }

        [Fact]
        public async Task ShouldMergeDuplicateSiblingKeys()
        {
            using var stream = new MemoryStream();
            // Tree type-code + non-null value flag.
            stream.WriteByte(DataType.Tree.TypeCode);
            stream.WriteByte(0x00);
            // Root tree with the same key written twice; each child is a leaf.
            WriteInt32(stream, 2);
            WriteFullyQualifiedString(stream, "dup");
            WriteInt32(stream, 0); // bare leaf child
            WriteFullyQualifiedString(stream, "dup");
            WriteInt32(stream, 0); // bare leaf child
            stream.Position = 0;

            var reader = new GraphBinaryReader();
            var result = await reader.ReadAsync(stream);

            var tree = Assert.IsType<Tree>(result);
            // The duplicate sibling keys collapse into a single merged entry.
            Assert.Single(tree.RootNodes());
            Assert.True(tree.HasChild("dup"));
        }

        private static void WriteFullyQualifiedString(Stream stream, string value)
        {
            stream.WriteByte(DataType.String.TypeCode);
            stream.WriteByte(0x00); // value flag: not null
            var bytes = Encoding.UTF8.GetBytes(value);
            WriteInt32(stream, bytes.Length);
            stream.Write(bytes, 0, bytes.Length);
        }

        private static void WriteInt32(Stream stream, int value)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            stream.Write(bytes, 0, 4);
        }

        private static async Task<Tree> RoundTrip(Tree tree)
        {
            var writer = new GraphBinaryWriter();
            var reader = new GraphBinaryReader();
            using var stream = new MemoryStream();
            await writer.WriteAsync(tree, stream);
            stream.Position = 0;
            var result = await reader.ReadAsync(stream);
            Assert.NotNull(result);
            return Assert.IsType<Tree>(result);
        }
    }
}
