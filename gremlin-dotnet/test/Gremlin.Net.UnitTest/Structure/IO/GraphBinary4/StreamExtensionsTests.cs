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
    public class StreamExtensionsTests
    {
        // --- EOF handling tests ---

        [Fact]
        public async Task ReadByteAsyncShouldThrowOnEmptyStream()
        {
            using var stream = new MemoryStream(Array.Empty<byte>());

            await Assert.ThrowsAsync<IOException>(
                async () => await stream.ReadByteAsync());
        }

        [Fact]
        public async Task ReadIntAsyncShouldThrowOnStreamWithFewerThan4Bytes()
        {
            using var stream = new MemoryStream(new byte[] { 0x01, 0x02, 0x03 });

            await Assert.ThrowsAsync<EndOfStreamException>(
                async () => await stream.ReadIntAsync());
        }

        [Fact]
        public async Task ReadLongAsyncShouldThrowOnStreamWithFewerThan8Bytes()
        {
            using var stream = new MemoryStream(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 });

            await Assert.ThrowsAsync<EndOfStreamException>(
                async () => await stream.ReadLongAsync());
        }

        [Fact]
        public async Task ReadShortAsyncShouldThrowOnStreamWithFewerThan2Bytes()
        {
            using var stream = new MemoryStream(new byte[] { 0x01 });

            await Assert.ThrowsAsync<EndOfStreamException>(
                async () => await stream.ReadShortAsync());
        }

        [Fact]
        public async Task ReadFloatAsyncShouldThrowOnStreamWithFewerThan4Bytes()
        {
            using var stream = new MemoryStream(new byte[] { 0x01, 0x02, 0x03 });

            await Assert.ThrowsAsync<EndOfStreamException>(
                async () => await stream.ReadFloatAsync());
        }

        [Fact]
        public async Task ReadDoubleAsyncShouldThrowOnStreamWithFewerThan8Bytes()
        {
            using var stream = new MemoryStream(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 });

            await Assert.ThrowsAsync<EndOfStreamException>(
                async () => await stream.ReadDoubleAsync());
        }

        [Fact]
        public async Task ReadAsyncShouldThrowOnStreamWithFewerThanCountBytes()
        {
            using var stream = new MemoryStream(new byte[] { 0x01, 0x02 });

            await Assert.ThrowsAsync<EndOfStreamException>(
                async () => await stream.ReadAsync(5));
        }

        // --- Valid big-endian read tests ---

        [Fact]
        public async Task ReadByteAsyncShouldReturnCorrectValue()
        {
            using var stream = new MemoryStream(new byte[] { 0xAB });

            var result = await stream.ReadByteAsync();

            Assert.Equal(0xAB, result);
        }

        [Fact]
        public async Task ReadIntAsyncShouldReturnCorrectBigEndianValue()
        {
            // Big-endian representation of 0x01020304 = 16909060
            using var stream = new MemoryStream(new byte[] { 0x01, 0x02, 0x03, 0x04 });

            var result = await stream.ReadIntAsync();

            Assert.Equal(0x01020304, result);
        }

        [Fact]
        public async Task ReadIntAsyncShouldHandleNegativeValue()
        {
            // Big-endian representation of -1 = 0xFFFFFFFF
            using var stream = new MemoryStream(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF });

            var result = await stream.ReadIntAsync();

            Assert.Equal(-1, result);
        }

        [Fact]
        public async Task ReadLongAsyncShouldReturnCorrectBigEndianValue()
        {
            // Big-endian representation of 0x0102030405060708
            using var stream = new MemoryStream(
                new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 });

            var result = await stream.ReadLongAsync();

            Assert.Equal(0x0102030405060708L, result);
        }

        [Fact]
        public async Task ReadLongAsyncShouldHandleNegativeValue()
        {
            // Big-endian representation of -1 = 0xFFFFFFFFFFFFFFFF
            using var stream = new MemoryStream(
                new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });

            var result = await stream.ReadLongAsync();

            Assert.Equal(-1L, result);
        }

        [Fact]
        public async Task ReadShortAsyncShouldReturnCorrectBigEndianValue()
        {
            // Big-endian representation of 0x0102 = 258
            using var stream = new MemoryStream(new byte[] { 0x01, 0x02 });

            var result = await stream.ReadShortAsync();

            Assert.Equal((short)0x0102, result);
        }

        [Fact]
        public async Task ReadShortAsyncShouldHandleNegativeValue()
        {
            // Big-endian representation of -1 = 0xFFFF
            using var stream = new MemoryStream(new byte[] { 0xFF, 0xFF });

            var result = await stream.ReadShortAsync();

            Assert.Equal((short)-1, result);
        }

        [Fact]
        public async Task ReadFloatAsyncShouldReturnCorrectBigEndianValue()
        {
            // Big-endian representation of 1.0f = 0x3F800000
            using var stream = new MemoryStream(new byte[] { 0x3F, 0x80, 0x00, 0x00 });

            var result = await stream.ReadFloatAsync();

            Assert.Equal(1.0f, result);
        }

        [Fact]
        public async Task ReadDoubleAsyncShouldReturnCorrectBigEndianValue()
        {
            // Big-endian representation of 1.0d = 0x3FF0000000000000
            using var stream = new MemoryStream(
                new byte[] { 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });

            var result = await stream.ReadDoubleAsync();

            Assert.Equal(1.0d, result);
        }

        [Fact]
        public async Task ReadAsyncShouldReturnCorrectBytes()
        {
            var expected = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
            using var stream = new MemoryStream(expected);

            var result = await stream.ReadAsync(4);

            Assert.Equal(expected, result);
        }

        // --- Round-trip tests (write then read) ---

        [Fact]
        public async Task IntRoundTripShouldPreserveValue()
        {
            using var stream = new MemoryStream();
            await stream.WriteIntAsync(42);
            stream.Position = 0;

            var result = await stream.ReadIntAsync();

            Assert.Equal(42, result);
        }

        [Fact]
        public async Task LongRoundTripShouldPreserveValue()
        {
            using var stream = new MemoryStream();
            await stream.WriteLongAsync(123456789012345L);
            stream.Position = 0;

            var result = await stream.ReadLongAsync();

            Assert.Equal(123456789012345L, result);
        }

        [Fact]
        public async Task ShortRoundTripShouldPreserveValue()
        {
            using var stream = new MemoryStream();
            await stream.WriteShortAsync(12345);
            stream.Position = 0;

            var result = await stream.ReadShortAsync();

            Assert.Equal((short)12345, result);
        }

        [Fact]
        public async Task FloatRoundTripShouldPreserveValue()
        {
            using var stream = new MemoryStream();
            await stream.WriteFloatAsync(3.14f);
            stream.Position = 0;

            var result = await stream.ReadFloatAsync();

            Assert.Equal(3.14f, result);
        }

        [Fact]
        public async Task DoubleRoundTripShouldPreserveValue()
        {
            using var stream = new MemoryStream();
            await stream.WriteDoubleAsync(3.14159265358979);
            stream.Position = 0;

            var result = await stream.ReadDoubleAsync();

            Assert.Equal(3.14159265358979, result);
        }

        // --- Bool read/write tests ---

        [Fact]
        public async Task ReadBoolAsyncShouldReturnTrueForByte1()
        {
            using var stream = new MemoryStream(new byte[] { 0x01 });

            var result = await stream.ReadBoolAsync();

            Assert.True(result);
        }

        [Fact]
        public async Task ReadBoolAsyncShouldReturnFalseForByte0()
        {
            using var stream = new MemoryStream(new byte[] { 0x00 });

            var result = await stream.ReadBoolAsync();

            Assert.False(result);
        }

        [Fact]
        public async Task ReadBoolAsyncShouldThrowOnInvalidByte()
        {
            using var stream = new MemoryStream(new byte[] { 0x02 });

            await Assert.ThrowsAsync<IOException>(
                async () => await stream.ReadBoolAsync());
        }

        [Fact]
        public async Task ReadBoolAsyncShouldThrowOnEmptyStream()
        {
            using var stream = new MemoryStream(Array.Empty<byte>());

            await Assert.ThrowsAsync<IOException>(
                async () => await stream.ReadBoolAsync());
        }

        [Fact]
        public async Task BoolRoundTripShouldPreserveTrueValue()
        {
            using var stream = new MemoryStream();
            await stream.WriteBoolAsync(true);
            stream.Position = 0;

            var result = await stream.ReadBoolAsync();

            Assert.True(result);
        }

        [Fact]
        public async Task BoolRoundTripShouldPreserveFalseValue()
        {
            using var stream = new MemoryStream();
            await stream.WriteBoolAsync(false);
            stream.Position = 0;

            var result = await stream.ReadBoolAsync();

            Assert.False(result);
        }

        // --- SByte read/write tests ---

        [Fact]
        public async Task ReadSByteAsyncShouldReturnCorrectPositiveValue()
        {
            using var stream = new MemoryStream(new byte[] { 0x7F }); // sbyte max = 127

            var result = await stream.ReadSByteAsync();

            Assert.Equal((sbyte)127, result);
        }

        [Fact]
        public async Task ReadSByteAsyncShouldReturnCorrectNegativeValue()
        {
            using var stream = new MemoryStream(new byte[] { 0x80 }); // sbyte min = -128

            var result = await stream.ReadSByteAsync();

            Assert.Equal((sbyte)-128, result);
        }

        [Fact]
        public async Task ReadSByteAsyncShouldThrowOnEmptyStream()
        {
            using var stream = new MemoryStream(Array.Empty<byte>());

            await Assert.ThrowsAsync<IOException>(
                async () => await stream.ReadSByteAsync());
        }

        [Fact]
        public async Task SByteRoundTripShouldPreserveValue()
        {
            using var stream = new MemoryStream();
            await stream.WriteSByteAsync(-42);
            stream.Position = 0;

            var result = await stream.ReadSByteAsync();

            Assert.Equal((sbyte)-42, result);
        }
    }
}
