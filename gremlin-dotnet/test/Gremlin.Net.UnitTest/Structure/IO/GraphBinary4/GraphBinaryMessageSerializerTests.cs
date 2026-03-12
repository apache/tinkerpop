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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    public class GraphBinaryMessageSerializerTests
    {
        [Fact]
        public async Task ShouldSerializeRequestMessageStartingWithVersionByte()
        {
            var msg = RequestMessage.Build("g.V()").AddG("g").Create();
            var serializer = CreateMessageSerializer();

            var actual = await serializer.SerializeMessageAsync(msg);

            // First byte should be version byte 0x84 — no MIME prefix
            Assert.Equal(0x84, actual[0]);
        }

        [Fact]
        public async Task ShouldNotIncludeMimePrefixInSerializedMessage()
        {
            var msg = RequestMessage.Build("g.V()").AddG("g").Create();
            var serializer = CreateMessageSerializer();

            var actual = await serializer.SerializeMessageAsync(msg);

            // The first byte is the version byte (0x84), not a MIME length byte
            Assert.True(actual[0] >> 7 == 1, "First byte should have MSB set (version byte)");
        }

        [Fact]
        public async Task SerializeMessageAsyncShouldSupportCancellation()
        {
            var serializer = CreateMessageSerializer();

            await Assert.ThrowsAsync<TaskCanceledException>(async () =>
                await serializer.SerializeMessageAsync(RequestMessage.Build(string.Empty).Create(),
                    new CancellationToken(true)));
        }

        [Fact]
        public async Task DeserializeMessageAsyncShouldSupportCancellation()
        {
            var serializer = CreateMessageSerializer();

            await Assert.ThrowsAsync<TaskCanceledException>(async () =>
                await serializer.DeserializeMessageAsync(Array.Empty<byte>(), new CancellationToken(true)));
        }

        private static GraphBinaryMessageSerializer CreateMessageSerializer()
        {
            return new GraphBinaryMessageSerializer();
        }
    }
}
