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
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    public class ResponseMessageSerializerTests
    {
        [Fact]
        public async Task ShouldDeserializeNonBulkedResponse()
        {
            // Build a response: version(0x81) + bulked(0x00) + int value 42 + marker + status footer
            using var stream = new MemoryStream();
            var writer = new GraphBinaryWriter();

            // Version byte
            await stream.WriteByteAsync(0x81);
            // Bulked = false
            await stream.WriteByteAsync(0x00);
            // Fully-qualified int value: type_code(0x01) + value_flag(0x00) + int32(42)
            await stream.WriteByteAsync(0x01); // DataType.Int
            await stream.WriteByteAsync(0x00); // value_flag = not null
            await stream.WriteIntAsync(42);
            // Marker: type_code(0xFD) + value_flag(0x00) + value(0x00)
            await stream.WriteByteAsync(0xFD);
            await stream.WriteByteAsync(0x00); // value_flag
            await stream.WriteByteAsync(0x00); // marker value
            // Status footer: status code 200
            await stream.WriteIntAsync(200);
            // Nullable status message: null (value_flag = 1)
            await stream.WriteByteAsync(0x01);
            // Nullable exception: null (value_flag = 1)
            await stream.WriteByteAsync(0x01);

            stream.Position = 0;
            var reader = new GraphBinaryReader();
            var serializer = new ResponseMessageSerializer();

            var result = await serializer.ReadValueAsync(stream, reader);

            Assert.False(result.Bulked);
            Assert.Single(result.Result);
            Assert.Equal(42, result.Result[0]);
            Assert.Equal(200, result.StatusCode);
            Assert.Null(result.StatusMessage);
            Assert.Null(result.Exception);
        }

        [Fact]
        public async Task ShouldDeserializeBulkedResponseWithTraverserWrapping()
        {
            using var stream = new MemoryStream();

            // Version byte
            await stream.WriteByteAsync(0x81);
            // Bulked = true
            await stream.WriteByteAsync(0x01);
            // Fully-qualified string value: type_code(0x03) + value_flag(0x00) + string "hello"
            await stream.WriteByteAsync(0x03); // DataType.String
            await stream.WriteByteAsync(0x00); // value_flag
            var helloBytes = System.Text.Encoding.UTF8.GetBytes("hello");
            await stream.WriteIntAsync(helloBytes.Length);
            await stream.WriteAsync(helloBytes);
            // Bulk count as fully-qualified Long: type_code(0x02) + value_flag(0x00) + long(3)
            await stream.WriteByteAsync(0x02); // DataType.Long
            await stream.WriteByteAsync(0x00); // value_flag = not null
            await stream.WriteLongAsync(3);
            // Marker: type_code(0xFD) + value_flag(0x00) + value(0x00)
            await stream.WriteByteAsync(0xFD);
            await stream.WriteByteAsync(0x00); // value_flag
            await stream.WriteByteAsync(0x00); // marker value
            // Status footer
            await stream.WriteIntAsync(200);
            await stream.WriteByteAsync(0x01); // null message
            await stream.WriteByteAsync(0x01); // null exception

            stream.Position = 0;
            var reader = new GraphBinaryReader();
            var serializer = new ResponseMessageSerializer();

            var result = await serializer.ReadValueAsync(stream, reader);

            Assert.True(result.Bulked);
            Assert.Single(result.Result);
            var traverser = Assert.IsType<Traverser>(result.Result[0]);
            Assert.Equal("hello", (string)traverser.Object);
            Assert.Equal(3L, traverser.Bulk);
        }

        [Fact]
        public async Task ShouldThrowResponseExceptionOnErrorStatus()
        {
            using var stream = new MemoryStream();

            // Version byte
            await stream.WriteByteAsync(0x81);
            // Bulked = false
            await stream.WriteByteAsync(0x00);
            // No result data — go straight to marker: type_code(0xFD) + value_flag(0x00) + value(0x00)
            await stream.WriteByteAsync(0xFD);
            await stream.WriteByteAsync(0x00); // value_flag
            await stream.WriteByteAsync(0x00); // marker value
            // Status footer: status code 500
            await stream.WriteIntAsync(500);
            // Status message: "Server error"
            await stream.WriteByteAsync(0x00); // value_flag = not null
            var msgBytes = System.Text.Encoding.UTF8.GetBytes("Server error");
            await stream.WriteIntAsync(msgBytes.Length);
            await stream.WriteAsync(msgBytes);
            // Exception: "java.lang.RuntimeException"
            await stream.WriteByteAsync(0x00); // value_flag = not null
            var excBytes = System.Text.Encoding.UTF8.GetBytes("java.lang.RuntimeException");
            await stream.WriteIntAsync(excBytes.Length);
            await stream.WriteAsync(excBytes);

            stream.Position = 0;
            var reader = new GraphBinaryReader();
            var serializer = new ResponseMessageSerializer();

            var ex = await Assert.ThrowsAsync<ResponseException>(
                () => serializer.ReadValueAsync(stream, reader));

            Assert.Equal(500, ex.StatusCode);
            Assert.Equal("Server error", ex.Message);
            Assert.Equal("java.lang.RuntimeException", ex.ServerException);
        }

        [Fact]
        public async Task ShouldThrowOnInvalidVersionByte()
        {
            using var stream = new MemoryStream();
            // Invalid version byte (MSB not set)
            await stream.WriteByteAsync(0x01);

            stream.Position = 0;
            var reader = new GraphBinaryReader();
            var serializer = new ResponseMessageSerializer();

            await Assert.ThrowsAsync<IOException>(
                () => serializer.ReadValueAsync(stream, reader));
        }

        [Fact]
        public async Task ShouldDeserializeMultipleNonBulkedResults()
        {
            using var stream = new MemoryStream();

            // Version + non-bulked
            await stream.WriteByteAsync(0x81);
            await stream.WriteByteAsync(0x00);
            // First result: int 1
            await stream.WriteByteAsync(0x01);
            await stream.WriteByteAsync(0x00);
            await stream.WriteIntAsync(1);
            // Second result: int 2
            await stream.WriteByteAsync(0x01);
            await stream.WriteByteAsync(0x00);
            await stream.WriteIntAsync(2);
            // Third result: int 3
            await stream.WriteByteAsync(0x01);
            await stream.WriteByteAsync(0x00);
            await stream.WriteIntAsync(3);
            // Marker: type_code(0xFD) + value_flag(0x00) + value(0x00)
            await stream.WriteByteAsync(0xFD);
            await stream.WriteByteAsync(0x00); // value_flag
            await stream.WriteByteAsync(0x00); // marker value
            // Status footer: 200, null, null
            await stream.WriteIntAsync(200);
            await stream.WriteByteAsync(0x01);
            await stream.WriteByteAsync(0x01);

            stream.Position = 0;
            var reader = new GraphBinaryReader();
            var serializer = new ResponseMessageSerializer();

            var result = await serializer.ReadValueAsync(stream, reader);

            Assert.Equal(3, result.Result.Count);
            Assert.Equal(1, result.Result[0]);
            Assert.Equal(2, result.Result[1]);
            Assert.Equal(3, result.Result[2]);
        }
    }
}
