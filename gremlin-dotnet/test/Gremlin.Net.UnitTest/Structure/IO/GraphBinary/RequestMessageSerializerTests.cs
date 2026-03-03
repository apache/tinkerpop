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

using System.IO;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO.GraphBinary;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary
{
    public class RequestMessageSerializerTests
    {
        [Fact]
        public async Task ShouldWriteVersionByteFirst()
        {
            var msg = RequestMessage.Build("g.V()").AddG("g").Create();
            var serializer = new RequestMessageSerializer();
            var writer = new GraphBinaryWriter();
            using var stream = new MemoryStream();

            await serializer.WriteValueAsync(msg, stream, writer);

            var bytes = stream.ToArray();
            Assert.Equal(0x81, bytes[0]);
        }

        [Fact]
        public async Task ShouldNotIncludeMimePrefix()
        {
            var msg = RequestMessage.Build("g.V()").AddG("g").Create();
            var serializer = new RequestMessageSerializer();
            var writer = new GraphBinaryWriter();
            using var stream = new MemoryStream();

            await serializer.WriteValueAsync(msg, stream, writer);

            var bytes = stream.ToArray();
            // First byte is version byte (MSB set), not a MIME length
            Assert.True(bytes[0] >> 7 == 1);
        }

        [Fact]
        public async Task ShouldSerializeFieldsAndGremlin()
        {
            var msg = RequestMessage.Build("g.V()").AddG("g").Create();
            var serializer = new RequestMessageSerializer();
            var writer = new GraphBinaryWriter();
            using var stream = new MemoryStream();

            await serializer.WriteValueAsync(msg, stream, writer);

            var bytes = stream.ToArray();
            // Should have more than just the version byte
            Assert.True(bytes.Length > 1);
            // Version byte
            Assert.Equal(0x81, bytes[0]);
        }

        [Fact]
        public async Task ShouldSerializeViaGraphBinaryMessageSerializer()
        {
            var msg = RequestMessage.Build("g.V()").AddG("g").Create();
            var serializer = new GraphBinaryMessageSerializer();

            var bytes = await serializer.SerializeMessageAsync(msg);

            // First byte is version byte, no MIME prefix
            Assert.Equal(0x81, bytes[0]);
            Assert.True(bytes.Length > 1);
        }
    }
}
