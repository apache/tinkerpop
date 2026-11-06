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
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary4
{
    public class GraphBinaryReadBufferTests
    {
        // --- Test doubles ---

        /// <summary>
        ///     A stream that returns at most <c>chunkSize</c> bytes per underlying ReadAsync call and always
        ///     completes asynchronously, forcing the buffer to refill in small increments so that reads
        ///     straddle refill boundaries.
        /// </summary>
        private sealed class DripStream : Stream
        {
            private readonly byte[] _data;
            private readonly int _chunkSize;
            private int _pos;

            public DripStream(byte[] data, int chunkSize)
            {
                _data = data;
                _chunkSize = chunkSize;
            }

            public override bool CanRead => true;
            public override bool CanWrite => false;
            public override bool CanSeek => false;
            public override long Length => throw new NotSupportedException();
            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override void Flush()
            {
            }

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override int Read(byte[] buffer, int offset, int count)
            {
                var remaining = _data.Length - _pos;
                if (remaining <= 0)
                {
                    return 0;
                }
                var toCopy = Math.Min(Math.Min(_chunkSize, count), remaining);
                Array.Copy(_data, _pos, buffer, offset, toCopy);
                _pos += toCopy;
                return toCopy;
            }

            public override async ValueTask<int> ReadAsync(Memory<byte> buffer,
                CancellationToken cancellationToken = default)
            {
                await Task.Yield();
                cancellationToken.ThrowIfCancellationRequested();
                var remaining = _data.Length - _pos;
                if (remaining <= 0)
                {
                    return 0;
                }
                var toCopy = Math.Min(Math.Min(_chunkSize, buffer.Length), remaining);
                _data.AsSpan(_pos, toCopy).CopyTo(buffer.Span);
                _pos += toCopy;
                return toCopy;
            }
        }

        /// <summary>
        ///     A stream whose ReadAsync never completes until the cancellation token trips.
        /// </summary>
        private sealed class StallingStream : Stream
        {
            public override bool CanRead => true;
            public override bool CanWrite => false;
            public override bool CanSeek => false;
            public override long Length => throw new NotSupportedException();
            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override void Flush()
            {
            }

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override async ValueTask<int> ReadAsync(Memory<byte> buffer,
                CancellationToken cancellationToken = default)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                return 0;
            }
        }

        /// <summary>
        ///     A stream that records whether Dispose was called; used to prove GraphBinaryReadBuffer does not
        ///     dispose the underlying stream.
        /// </summary>
        private sealed class ObservableStream : Stream
        {
            private readonly MemoryStream _inner;
            public bool Disposed { get; private set; }

            public ObservableStream(byte[] data)
            {
                _inner = new MemoryStream(data);
            }

            public override bool CanRead => true;
            public override bool CanWrite => false;
            public override bool CanSeek => false;
            public override long Length => throw new NotSupportedException();
            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override void Flush()
            {
            }

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

            public override ValueTask<int> ReadAsync(Memory<byte> buffer,
                CancellationToken cancellationToken = default) => _inner.ReadAsync(buffer, cancellationToken);

            protected override void Dispose(bool disposing)
            {
                Disposed = true;
                _inner.Dispose();
                base.Dispose(disposing);
            }
        }

        /// <summary>
        ///     A stream whose ReadAsync always throws. Used to prove that a failed underlying refill
        ///     surfaces the exception cleanly and does not leave the buffer in a half-consumed state.
        /// </summary>
        private sealed class ThrowingStream : Stream
        {
            public override bool CanRead => true;
            public override bool CanWrite => false;
            public override bool CanSeek => false;
            public override long Length => throw new NotSupportedException();
            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override void Flush()
            {
            }

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override ValueTask<int> ReadAsync(Memory<byte> buffer,
                CancellationToken cancellationToken = default)
                => throw new IOException("simulated underlying stream failure");
        }

        private static GraphBinaryReadBuffer NewBuffer(byte[] data)
            => new GraphBinaryReadBuffer(new MemoryStream(data));

        // --- 1. Already-buffered reads (no I/O) ---

        [Fact]
        public async Task ReadByteValueShouldReturnCorrectValueOnHitPath()
        {
            var buffer = NewBuffer(new byte[] { 0xAB });
            Assert.Equal(0xAB, await buffer.ReadByteValueAsync());
        }

        [Fact]
        public async Task ReadIntValueShouldReturnCorrectBigEndianValueOnHitPath()
        {
            var buffer = NewBuffer(new byte[] { 0x01, 0x02, 0x03, 0x04 });
            Assert.Equal(0x01020304, await buffer.ReadIntValueAsync());
        }

        [Fact]
        public async Task ReadIntValueShouldHandleAllOnesOnHitPath()
        {
            var buffer = NewBuffer(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF });
            Assert.Equal(-1, await buffer.ReadIntValueAsync());
        }

        [Fact]
        public async Task ReadLongValueShouldReturnCorrectBigEndianValueOnHitPath()
        {
            var buffer = NewBuffer(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 });
            Assert.Equal(0x0102030405060708L, await buffer.ReadLongValueAsync());
        }

        [Fact]
        public async Task ReadLongValueShouldHandleAllOnesOnHitPath()
        {
            var buffer = NewBuffer(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });
            Assert.Equal(-1L, await buffer.ReadLongValueAsync());
        }

        [Fact]
        public async Task ReadShortValueShouldReturnCorrectBigEndianValueOnHitPath()
        {
            var buffer = NewBuffer(new byte[] { 0x01, 0x02 });
            Assert.Equal((short)0x0102, await buffer.ReadShortValueAsync());
        }

        [Fact]
        public async Task ReadShortValueShouldHandleAllOnesOnHitPath()
        {
            var buffer = NewBuffer(new byte[] { 0xFF, 0xFF });
            Assert.Equal((short)-1, await buffer.ReadShortValueAsync());
        }

        [Fact]
        public async Task ReadFloatValueShouldReturnCorrectBigEndianValueOnHitPath()
        {
            var buffer = NewBuffer(new byte[] { 0x3F, 0x80, 0x00, 0x00 }); // 1.0f
            Assert.Equal(1.0f, await buffer.ReadFloatValueAsync());
        }

        [Fact]
        public async Task ReadDoubleValueShouldReturnCorrectBigEndianValueOnHitPath()
        {
            var buffer = NewBuffer(new byte[] { 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }); // 1.0d
            Assert.Equal(1.0d, await buffer.ReadDoubleValueAsync());
        }

        // --- 2. Boundary path: a value spans a refill boundary ---

        private static byte[] IntBytes(int value)
        {
            var b = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(b, value);
            return b;
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task ReadIntValueShouldBeCorrectAcrossRefillViaDripStream(int chunkSize)
        {
            var buffer = new GraphBinaryReadBuffer(new DripStream(IntBytes(0x0A0B0C0D), chunkSize));
            Assert.Equal(0x0A0B0C0D, await buffer.ReadIntValueAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task ReadIntValueShouldBeCorrectAcrossRefillViaTinyBuffer(int bufferSize)
        {
            var buffer = new GraphBinaryReadBuffer(new MemoryStream(IntBytes(0x0A0B0C0D)), bufferSize);
            Assert.Equal(0x0A0B0C0D, await buffer.ReadIntValueAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task ReadLongValueShouldBeCorrectAcrossRefill(int chunkSize)
        {
            var data = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(data, 0x1122334455667788L);
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, chunkSize));
            Assert.Equal(0x1122334455667788L, await buffer.ReadLongValueAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task ReadDoubleValueShouldBeCorrectAcrossRefill(int chunkSize)
        {
            var data = new byte[8];
            BinaryPrimitives.WriteDoubleBigEndian(data, 3.14159265358979);
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, chunkSize));
            Assert.Equal(3.14159265358979, await buffer.ReadDoubleValueAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        public async Task ReadShortValueShouldBeCorrectAcrossRefill(int chunkSize)
        {
            var data = new byte[2];
            BinaryPrimitives.WriteInt16BigEndian(data, unchecked((short)0xBEEF));
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, chunkSize));
            Assert.Equal(unchecked((short)0xBEEF), await buffer.ReadShortValueAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        public async Task ReadFloatValueShouldBeCorrectAcrossRefill(int chunkSize)
        {
            var data = new byte[4];
            BinaryPrimitives.WriteSingleBigEndian(data, 3.14f);
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, chunkSize));
            Assert.Equal(3.14f, await buffer.ReadFloatValueAsync());
        }

        [Fact]
        public async Task ReadByteValueShouldBeCorrectAcrossRefillViaTinyBuffer()
        {
            // A one-byte value read after buffer is exhausted forces the slow path.
            var buffer = new GraphBinaryReadBuffer(new DripStream(new byte[] { 0x11, 0x22 }, 1), 1);
            Assert.Equal(0x11, await buffer.ReadByteValueAsync());
            Assert.Equal(0x22, await buffer.ReadByteValueAsync());
        }

        // --- 3. Mixed-width record crossing refill boundaries ---

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task MixedWidthRecordShouldReassembleAcrossRefills(int chunkSize)
        {
            // Lay out int, long, byte, short, float, double back-to-back.
            using var ms = new MemoryStream();
            var scratch = new byte[8];

            BinaryPrimitives.WriteInt32BigEndian(scratch.AsSpan(0, 4), -123456);
            ms.Write(scratch, 0, 4);

            BinaryPrimitives.WriteInt64BigEndian(scratch, 0x0102030405060708L);
            ms.Write(scratch, 0, 8);

            ms.WriteByte(0xC3);

            BinaryPrimitives.WriteInt16BigEndian(scratch.AsSpan(0, 2), unchecked((short)0xABCD));
            ms.Write(scratch, 0, 2);

            BinaryPrimitives.WriteSingleBigEndian(scratch.AsSpan(0, 4), 2.71828f);
            ms.Write(scratch, 0, 4);

            BinaryPrimitives.WriteDoubleBigEndian(scratch, 1.41421356237);
            ms.Write(scratch, 0, 8);

            var data = ms.ToArray();
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, chunkSize), 4);

            Assert.Equal(-123456, await buffer.ReadIntValueAsync());
            Assert.Equal(0x0102030405060708L, await buffer.ReadLongValueAsync());
            Assert.Equal(0xC3, await buffer.ReadByteValueAsync());
            Assert.Equal(unchecked((short)0xABCD), await buffer.ReadShortValueAsync());
            Assert.Equal(2.71828f, await buffer.ReadFloatValueAsync());
            Assert.Equal(1.41421356237, await buffer.ReadDoubleValueAsync());
        }

        // --- 4. EOF contract ---

        [Fact]
        public async Task ReadByteValueShouldThrowIOExceptionOnEmptyStream()
        {
            var buffer = NewBuffer(Array.Empty<byte>());
            await Assert.ThrowsAsync<IOException>(async () => await buffer.ReadByteValueAsync());
        }

        [Fact]
        public async Task ReadIntValueShouldThrowEndOfStreamOnShortStream()
        {
            var buffer = NewBuffer(new byte[] { 0x01, 0x02, 0x03 });
            await Assert.ThrowsAsync<EndOfStreamException>(async () => await buffer.ReadIntValueAsync());
        }

        [Fact]
        public async Task ReadLongValueShouldThrowEndOfStreamOnShortStream()
        {
            var buffer = NewBuffer(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 });
            await Assert.ThrowsAsync<EndOfStreamException>(async () => await buffer.ReadLongValueAsync());
        }

        [Fact]
        public async Task ReadShortValueShouldThrowEndOfStreamOnShortStream()
        {
            var buffer = NewBuffer(new byte[] { 0x01 });
            await Assert.ThrowsAsync<EndOfStreamException>(async () => await buffer.ReadShortValueAsync());
        }

        [Fact]
        public async Task ReadFloatValueShouldThrowEndOfStreamOnShortStream()
        {
            var buffer = NewBuffer(new byte[] { 0x01, 0x02 });
            await Assert.ThrowsAsync<EndOfStreamException>(async () => await buffer.ReadFloatValueAsync());
        }

        [Fact]
        public async Task ReadDoubleValueShouldThrowEndOfStreamOnShortStream()
        {
            var buffer = NewBuffer(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
            await Assert.ThrowsAsync<EndOfStreamException>(async () => await buffer.ReadDoubleValueAsync());
        }

        // --- 5. General ReadAsync path across refills ---

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task ReadAsyncCountShouldReturnCorrectBytesAcrossRefills(int chunkSize)
        {
            var expected = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06 };
            var buffer = new GraphBinaryReadBuffer(new DripStream(expected, chunkSize), 4);

            var result = await buffer.ReadAsync(expected.Length);

            Assert.Equal(expected, result);
        }

        [Fact]
        public async Task ReadAsyncCountShouldConsumeAfterPrimitiveReads()
        {
            using var ms = new MemoryStream();
            var scratch = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(scratch, 42);
            ms.Write(scratch, 0, 4);
            ms.Write(new byte[] { 0x0A, 0x0B, 0x0C }, 0, 3);

            var buffer = new GraphBinaryReadBuffer(new DripStream(ms.ToArray(), 2), 3);

            Assert.Equal(42, await buffer.ReadIntValueAsync());
            Assert.Equal(new byte[] { 0x0A, 0x0B, 0x0C }, await buffer.ReadAsync(3));
        }

        [Fact]
        public async Task ReadAsyncByteArrayShouldFillFromNonZeroOffsetAcrossRefills()
        {
            // Exercises the ReadAsync(byte[], offset, count) overload with a non-zero offset: the leading
            // guard byte must be left intact and the payload written starting at index 1, reassembled across
            // refills.
            var payload = new byte[] { 0x11, 0x22, 0x33, 0x44, 0x55 };
            var buffer = new GraphBinaryReadBuffer(new DripStream(payload, 2), 2);

            var dest = new byte[payload.Length + 1];
            dest[0] = 0xEE;
            var offset = 1;
            while (offset < dest.Length)
            {
                var n = await buffer.ReadAsync(dest, offset, dest.Length - offset, CancellationToken.None);
                Assert.True(n > 0);
                offset += n;
            }

            Assert.Equal(new byte[] { 0xEE, 0x11, 0x22, 0x33, 0x44, 0x55 }, dest);
        }

        // --- 6. Cancellation ---

        [Fact]
        public async Task PrimitiveBoundaryReadShouldHonorCancellation()
        {
            using var cts = new CancellationTokenSource();
            var buffer = new GraphBinaryReadBuffer(new StallingStream());
            var task = buffer.ReadIntValueAsync(cts.Token);
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await task);
        }

        [Fact]
        public async Task ReadAsyncCountShouldHonorCancellation()
        {
            using var cts = new CancellationTokenSource();
            var buffer = new GraphBinaryReadBuffer(new StallingStream());
            var task = buffer.ReadAsync(4, cts.Token);
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await task);
        }

        // --- 7. Dispose does not dispose the underlying stream ---

        [Fact]
        public void DisposeShouldNotDisposeUnderlyingStream()
        {
            var underlying = new ObservableStream(new byte[] { 0x01, 0x02, 0x03, 0x04 });
            var buffer = new GraphBinaryReadBuffer(underlying);

            buffer.Dispose();

            Assert.False(underlying.Disposed);
        }

        [Fact]
        public async Task DisposeAsyncShouldNotDisposeUnderlyingStream()
        {
            var underlying = new ObservableStream(new byte[] { 0x01, 0x02, 0x03, 0x04 });
            var buffer = new GraphBinaryReadBuffer(underlying);

            await buffer.DisposeAsync();

            Assert.False(underlying.Disposed);
        }

        // --- 8. Public StreamExtensions dispatch onto GraphBinaryReadBuffer ---
        //
        // These call the public extension methods, which internally check `stream is GraphBinaryReadBuffer`
        // and forward to the corresponding typed reader. A cross-wired dispatch (say, ReadShortAsync
        // routing to ReadIntValueAsync) would surface here. Each width uses a distinctive value so a
        // cross-wire produces a wrong result instead of a coincidental match.

        [Fact]
        public async Task PublicReadByteAsyncShouldDispatchToByteReader()
        {
            var buffer = NewBuffer(new byte[] { 0xAB });
            Assert.Equal(0xAB, await buffer.ReadByteAsync());
        }

        [Fact]
        public async Task PublicReadSByteAsyncShouldDispatchToByteReader()
        {
            var buffer = NewBuffer(new byte[] { 0xFF });
            Assert.Equal((sbyte)-1, await buffer.ReadSByteAsync());
        }

        [Fact]
        public async Task PublicReadBoolAsyncShouldDispatchToByteReader()
        {
            var buffer = NewBuffer(new byte[] { 0x01 });
            Assert.True(await buffer.ReadBoolAsync());
        }

        [Fact]
        public async Task PublicReadShortAsyncShouldDispatchToShortReader()
        {
            var buffer = NewBuffer(new byte[] { 0x01, 0x02 });
            Assert.Equal((short)0x0102, await buffer.ReadShortAsync());
        }

        [Fact]
        public async Task PublicReadIntAsyncShouldDispatchToIntReader()
        {
            var buffer = NewBuffer(new byte[] { 0x01, 0x02, 0x03, 0x04 });
            Assert.Equal(0x01020304, await buffer.ReadIntAsync());
        }

        [Fact]
        public async Task PublicReadLongAsyncShouldDispatchToLongReader()
        {
            var buffer = NewBuffer(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 });
            Assert.Equal(0x0102030405060708L, await buffer.ReadLongAsync());
        }

        [Fact]
        public async Task PublicReadFloatAsyncShouldDispatchToFloatReader()
        {
            var buffer = NewBuffer(new byte[] { 0x3F, 0x80, 0x00, 0x00 }); // 1.0f
            Assert.Equal(1.0f, await buffer.ReadFloatAsync());
        }

        [Fact]
        public async Task PublicReadDoubleAsyncShouldDispatchToDoubleReader()
        {
            var buffer = NewBuffer(new byte[] { 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }); // 1.0d
            Assert.Equal(1.0d, await buffer.ReadDoubleAsync());
        }

        // --- 9. Explicit partial-prefix boundary: k of N bytes buffered, N-k need a refill ---
        //
        // For each split point k (1..N-1) we prime the buffer so that a leading single-byte read leaves
        // exactly k bytes of the target value already buffered, and the remaining N-k bytes must be
        // fetched from the underlying (drip) stream. This exercises the slow path at every possible
        // split, plus a matching short-stream case that must throw EndOfStreamException.

        // Buffer size is chosen so that (1 leading byte + k value bytes) fit before a refill. The
        // leading 0xEE marker is consumed first, leaving k value bytes buffered and N-k to refill.
        private static byte[] PrefixedPrimitive(byte[] primitive)
        {
            var result = new byte[primitive.Length + 1];
            result[0] = 0xEE;
            Array.Copy(primitive, 0, result, 1, primitive.Length);
            return result;
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public async Task ReadShortValueShouldSplitAtEveryBoundary(int bufferSize)
        {
            var primitive = new byte[2];
            BinaryPrimitives.WriteInt16BigEndian(primitive, unchecked((short)0xBEEF));
            var data = PrefixedPrimitive(primitive);
            // Drip one byte at a time to guarantee refills land mid-value.
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, 1), bufferSize);

            Assert.Equal(0xEE, await buffer.ReadByteValueAsync());
            Assert.Equal(unchecked((short)0xBEEF), await buffer.ReadShortValueAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        public async Task ReadIntValueShouldSplitAtEveryBoundary(int bufferSize)
        {
            var data = PrefixedPrimitive(IntBytes(0x0A0B0C0D));
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, 1), bufferSize);

            Assert.Equal(0xEE, await buffer.ReadByteValueAsync());
            Assert.Equal(0x0A0B0C0D, await buffer.ReadIntValueAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        [InlineData(9)]
        public async Task ReadLongValueShouldSplitAtEveryBoundary(int bufferSize)
        {
            var primitive = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(primitive, 0x1122334455667788L);
            var data = PrefixedPrimitive(primitive);
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, 1), bufferSize);

            Assert.Equal(0xEE, await buffer.ReadByteValueAsync());
            Assert.Equal(0x1122334455667788L, await buffer.ReadLongValueAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        public async Task ReadFloatValueShouldSplitAtEveryBoundary(int bufferSize)
        {
            var primitive = new byte[4];
            BinaryPrimitives.WriteSingleBigEndian(primitive, 2.71828f);
            var data = PrefixedPrimitive(primitive);
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, 1), bufferSize);

            Assert.Equal(0xEE, await buffer.ReadByteValueAsync());
            Assert.Equal(2.71828f, await buffer.ReadFloatValueAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        [InlineData(9)]
        public async Task ReadDoubleValueShouldSplitAtEveryBoundary(int bufferSize)
        {
            var primitive = new byte[8];
            BinaryPrimitives.WriteDoubleBigEndian(primitive, 1.41421356237);
            var data = PrefixedPrimitive(primitive);
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, 1), bufferSize);

            Assert.Equal(0xEE, await buffer.ReadByteValueAsync());
            Assert.Equal(1.41421356237, await buffer.ReadDoubleValueAsync());
        }

        // Partial value then EOF: a leading byte is consumed, then only part of the value is available
        // before the stream ends, so the slow path throws EndOfStreamException.

        [Theory]
        [InlineData(1)] // 1 of 4 int bytes present after the leading byte
        [InlineData(2)]
        [InlineData(3)]
        public async Task ReadIntValueShouldThrowEndOfStreamOnPartialAfterPrefix(int primitiveBytesPresent)
        {
            var data = new byte[1 + primitiveBytesPresent];
            data[0] = 0xEE;
            for (var i = 0; i < primitiveBytesPresent; i++)
            {
                data[i + 1] = (byte)(i + 1);
            }
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, 1), 2);

            Assert.Equal(0xEE, await buffer.ReadByteValueAsync());
            await Assert.ThrowsAsync<EndOfStreamException>(async () => await buffer.ReadIntValueAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(4)]
        [InlineData(7)] // 7 of 8 long bytes present after the leading byte
        public async Task ReadLongValueShouldThrowEndOfStreamOnPartialAfterPrefix(int primitiveBytesPresent)
        {
            var data = new byte[1 + primitiveBytesPresent];
            data[0] = 0xEE;
            for (var i = 0; i < primitiveBytesPresent; i++)
            {
                data[i + 1] = (byte)(i + 1);
            }
            var buffer = new GraphBinaryReadBuffer(new DripStream(data, 1), 2);

            Assert.Equal(0xEE, await buffer.ReadByteValueAsync());
            await Assert.ThrowsAsync<EndOfStreamException>(async () => await buffer.ReadLongValueAsync());
        }

        // --- 10. General ReadAsync(count) delivering fewer than count bytes then EOF ---
        //
        // Proves the refill loop inside ReadExactlyAsync terminates (no hang / infinite loop) and
        // surfaces EndOfStreamException when the underlying stream ends early.

        [Fact]
        public async Task ReadAsyncCountShouldThrowEndOfStreamWhenStreamEndsEarly()
        {
            // Only 3 bytes available but 8 requested; DripStream returns 0 after exhausting its data.
            var buffer = new GraphBinaryReadBuffer(new DripStream(new byte[] { 0x01, 0x02, 0x03 }, 1), 2);
            await Assert.ThrowsAsync<EndOfStreamException>(async () => await buffer.ReadAsync(8));
        }

        // --- 11. Pre-cancelled token on a refill-forcing read ---
        //
        // With an already-cancelled token, a read whose buffer is empty must observe the cancellation
        // via the underlying ReadAsync and throw OperationCanceledException.
        //
        // Note: when the bytes are already buffered, the typed reader intentionally does not observe
        // the cancellation token. It does no I/O and returns synchronously. That behavior is pinned by
        // FullyBufferedFastPathShouldNotObservePreCancelledToken below.

        [Fact]
        public async Task PreCancelledTokenShouldThrowWhenRefillRequired()
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            // Empty internal buffer forces a refill, which must observe the cancelled token.
            var buffer = new GraphBinaryReadBuffer(new DripStream(new byte[] { 0x01, 0x02, 0x03, 0x04 }, 1));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await buffer.ReadIntValueAsync(cts.Token));
        }

        [Fact]
        public async Task FullyBufferedFastPathShouldNotObservePreCancelledToken()
        {
            using var cts = new CancellationTokenSource();
            // Prime the internal buffer with a first (uncancelled) read: this refill pulls all 8 bytes
            // from the MemoryStream into the default 8192-byte buffer.
            var buffer = NewBuffer(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 });
            Assert.Equal(0x01020304, await buffer.ReadIntValueAsync());

            // The remaining 4 bytes are already buffered, so the second read returns synchronously
            // without any I/O and does not observe the cancelled token. This is intentional; see the
            // note above.
            cts.Cancel();
            Assert.Equal(0x05060708, await buffer.ReadIntValueAsync(cts.Token));
        }

        // --- 12. End-to-end drip through ResponseSerializer over a non-seekable fragmented stream ---
        //
        // Mirrors ResponseSerializerTests payload construction, then replays the same bytes through a
        // DripStream so the response arrives in tiny chunks. This proves the buffering wrapper works
        // end-to-end on a fragmented, non-seekable stream (something no other test exercises).

        private static byte[] BuildNonBulkedIntResponse(params int[] values)
        {
            using var ms = new MemoryStream();
            ms.WriteByte(0x84); // version
            ms.WriteByte(0x00); // bulked = false
            var scratch = new byte[4];
            foreach (var value in values)
            {
                ms.WriteByte(0x01); // DataType.Int
                ms.WriteByte(0x00); // value_flag = not null
                BinaryPrimitives.WriteInt32BigEndian(scratch, value);
                ms.Write(scratch, 0, 4);
            }
            // Marker
            ms.WriteByte(0xFD);
            ms.WriteByte(0x00);
            ms.WriteByte(0x00);
            // Status footer: 200, null message, null exception
            BinaryPrimitives.WriteInt32BigEndian(scratch, 200);
            ms.Write(scratch, 0, 4);
            ms.WriteByte(0x01);
            ms.WriteByte(0x01);
            return ms.ToArray();
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(7)]
        public async Task ResponseSerializerShouldDeserializeAcrossFragmentedDripStream(int chunkSize)
        {
            var payload = BuildNonBulkedIntResponse(1, 2, 3, 42, -7, int.MaxValue, int.MinValue);
            var reader = new GraphBinaryReader();
            var serializer = new ResponseSerializer();

            // Baseline: full (non-dripped) MemoryStream.
            var baseline = new List<object>();
            await foreach (var item in serializer.ReadStreamingAsync(new MemoryStream(payload), reader))
            {
                baseline.Add(item);
            }

            // Fragmented: same bytes delivered chunkSize at a time through a non-seekable stream.
            var dripped = new List<object>();
            await foreach (var item in serializer.ReadStreamingAsync(new DripStream(payload, chunkSize), reader))
            {
                dripped.Add(item);
            }

            Assert.Equal(new object[] { 1, 2, 3, 42, -7, int.MaxValue, int.MinValue }, baseline);
            Assert.Equal(baseline, dripped);
        }

        // --- 13. Multi-byte value straddling a refill boundary (short-read regression guard) ---
        //
        // A general Read/ReadAsync legitimately returns only what is currently buffered, so a value read
        // (e.g. a string) that straddles the internal refill boundary comes back in more than one chunk.
        // Consumers that issue a single non-looping stream.ReadAsync(bytes, offset, count) and ignore the
        // returned count would silently truncate the value and desynchronize the parser. These tests build
        // a real GraphBinary String response whose payload crosses the buffer boundary and assert it still
        // round-trips end-to-end through ResponseSerializer.

        private static byte[] BuildNonBulkedStringResponse(params string[] values)
        {
            using var ms = new MemoryStream();
            ms.WriteByte(0x84); // version
            ms.WriteByte(0x00); // bulked = false
            var scratch = new byte[4];
            foreach (var value in values)
            {
                ms.WriteByte(0x03); // DataType.String
                ms.WriteByte(0x00); // value_flag = not null
                var utf8 = Encoding.UTF8.GetBytes(value);
                BinaryPrimitives.WriteInt32BigEndian(scratch, utf8.Length);
                ms.Write(scratch, 0, 4);
                ms.Write(utf8, 0, utf8.Length);
            }
            // Marker
            ms.WriteByte(0xFD);
            ms.WriteByte(0x00);
            ms.WriteByte(0x00);
            // Status footer: 200, null message, null exception
            BinaryPrimitives.WriteInt32BigEndian(scratch, 200);
            ms.Write(scratch, 0, 4);
            ms.WriteByte(0x01);
            ms.WriteByte(0x01);
            return ms.ToArray();
        }

        [Theory]
        [InlineData(4)]   // chunk far smaller than the string payload
        [InlineData(8)]
        [InlineData(16)]
        public async Task ResponseSerializerShouldDeserializeStringLargerThanBuffer(int chunkSize)
        {
            // A string whose UTF-8 length far exceeds the drip chunk size forces the payload to span
            // multiple refills. Feed it through a DripStream (not a pre-wrapped GraphBinaryReadBuffer)
            // so the chunk size directly controls the refill granularity of the buffer that
            // ResponseSerializer creates internally.
            var value = new string('x', 100);
            var payload = BuildNonBulkedStringResponse(value);
            var reader = new GraphBinaryReader();
            var serializer = new ResponseSerializer();

            var results = new List<object>();
            await foreach (var item in serializer.ReadStreamingAsync(
                new DripStream(payload, chunkSize), reader))
            {
                results.Add(item);
            }

            Assert.Equal(new object[] { value }, results);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(7)]
        public async Task ResponseSerializerShouldDeserializeStringsAcrossFragmentedDripStream(int chunkSize)
        {
            // Multiple strings of varying lengths, delivered a few bytes at a time through a non-seekable
            // stream, so almost every value straddles a refill boundary.
            var values = new[] { "", "a", "hello world", new string('z', 50), "τ" };
            var payload = BuildNonBulkedStringResponse(values);
            var reader = new GraphBinaryReader();
            var serializer = new ResponseSerializer();

            var results = new List<object>();
            await foreach (var item in serializer.ReadStreamingAsync(new DripStream(payload, chunkSize), reader))
            {
                results.Add(item);
            }

            Assert.Equal(values, results);
        }

        // CharSerializer reads a multi-byte UTF-8 char as a leading byte plus ReadExactlyAsync for the
        // rest. This is a distinct code path (type code 0x80) from the length-prefixed StringSerializer.
        // With a 1-byte drip the continuation bytes land after a refill, pinning that the read loops
        // correctly.
        private static byte[] BuildNonBulkedCharResponse(params char[] values)
        {
            using var ms = new MemoryStream();
            ms.WriteByte(0x84); // version
            ms.WriteByte(0x00); // bulked = false
            var scratch = new byte[4];
            foreach (var value in values)
            {
                ms.WriteByte(0x80); // DataType.Char
                ms.WriteByte(0x00); // value_flag = not null
                var utf8 = Encoding.UTF8.GetBytes(value.ToString());
                ms.Write(utf8, 0, utf8.Length); // Char has no length prefix; leading byte encodes width
            }
            ms.WriteByte(0xFD);
            ms.WriteByte(0x00);
            ms.WriteByte(0x00);
            BinaryPrimitives.WriteInt32BigEndian(scratch, 200);
            ms.Write(scratch, 0, 4);
            ms.WriteByte(0x01);
            ms.WriteByte(0x01);
            return ms.ToArray();
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public async Task ResponseSerializerShouldDeserializeMultiByteCharsAcrossRefills(int chunkSize)
        {
            // 'a' (1 byte), 'τ' (2 bytes, U+03C4), '€' (3 bytes, U+20AC). Exercises the 2- and 3-byte
            // CharSerializer branches whose continuation bytes straddle a refill under a tiny drip chunk.
            var values = new[] { 'a', 'τ', '€' };
            var payload = BuildNonBulkedCharResponse(values);
            var reader = new GraphBinaryReader();
            var serializer = new ResponseSerializer();

            var results = new List<object>();
            await foreach (var item in serializer.ReadStreamingAsync(new DripStream(payload, chunkSize), reader))
            {
                results.Add(item);
            }

            Assert.Equal(new object[] { 'a', 'τ', '€' }, results);
        }

        // BinarySerializer (byte[]) reads a length-prefixed payload via ReadExactlyAsync, the same shape
        // as StringSerializer; a large byte[] straddling refills exercises that read path directly.
        private static byte[] BuildNonBulkedBinaryResponse(params byte[][] values)
        {
            using var ms = new MemoryStream();
            ms.WriteByte(0x84); // version
            ms.WriteByte(0x00); // bulked = false
            var scratch = new byte[4];
            foreach (var value in values)
            {
                ms.WriteByte(0x25); // DataType.Binary
                ms.WriteByte(0x00); // value_flag = not null
                BinaryPrimitives.WriteInt32BigEndian(scratch, value.Length);
                ms.Write(scratch, 0, 4);
                ms.Write(value, 0, value.Length);
            }
            ms.WriteByte(0xFD);
            ms.WriteByte(0x00);
            ms.WriteByte(0x00);
            BinaryPrimitives.WriteInt32BigEndian(scratch, 200);
            ms.Write(scratch, 0, 4);
            ms.WriteByte(0x01);
            ms.WriteByte(0x01);
            return ms.ToArray();
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(7)]
        public async Task ResponseSerializerShouldDeserializeByteArrayLargerThanBuffer(int chunkSize)
        {
            var value = new byte[100];
            for (var i = 0; i < value.Length; i++)
            {
                value[i] = (byte)(i + 1); // non-zero so silent truncation (zero-fill) would be caught
            }
            var payload = BuildNonBulkedBinaryResponse(value);
            var reader = new GraphBinaryReader();
            var serializer = new ResponseSerializer();

            var results = new List<object>();
            await foreach (var item in serializer.ReadStreamingAsync(new DripStream(payload, chunkSize), reader))
            {
                results.Add(item);
            }

            var single = Assert.Single(results);
            Assert.Equal(value, Assert.IsType<byte[]>(single));
        }

        // --- 14. Constructor argument validation ---

        [Fact]
        public void ConstructorShouldThrowOnNullStream()
        {
            Assert.Throws<ArgumentNullException>(() => new GraphBinaryReadBuffer(null!));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void ConstructorShouldThrowOnNonPositiveBufferSize(int bufferSize)
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new GraphBinaryReadBuffer(new MemoryStream(), bufferSize));
        }

        // --- 15. Zero-length read early-return ---

        [Fact]
        public async Task ReadAsyncEmptyBufferShouldReturnZeroWithoutConsuming()
        {
            var buffer = NewBuffer(new byte[] { 0xAB });

            Assert.Equal(0, await buffer.ReadAsync(Array.Empty<byte>(), 0, 0));

            // The single available byte must be untouched by the zero-length read.
            Assert.Equal(0xAB, await buffer.ReadByteValueAsync());
        }

        // --- 16. General ReadAsync(count) then a fixed-width read that straddles a refill ---
        //
        // The reverse of ReadAsyncCountShouldConsumeAfterPrimitiveReads: a general read leaves the buffer
        // partially consumed at a non-zero _start, and the following fixed-width read must span from
        // that mid-buffer offset across a refill. Exercises the _start/_end accounting most prone to
        // off-by-one.

        [Fact]
        public async Task PrimitiveReadShouldBeCorrectAfterGeneralReadLeavesPartialBuffer()
        {
            using var ms = new MemoryStream();
            ms.Write(new byte[] { 0x0A, 0x0B, 0x0C }, 0, 3);
            var scratch = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(scratch, 0x1122334455667788L);
            ms.Write(scratch, 0, 8);

            // Small buffer + drip so the long straddles refills starting from a non-zero _start.
            var buffer = new GraphBinaryReadBuffer(new DripStream(ms.ToArray(), 2), 4);

            Assert.Equal(new byte[] { 0x0A, 0x0B, 0x0C }, await buffer.ReadAsync(3));
            Assert.Equal(0x1122334455667788L, await buffer.ReadLongValueAsync());
        }

        // --- 17. Refill exception propagation ---
        //
        // RefillAsync commits _start/_end only after the awaited underlying read returns, so a thrown
        // exception must surface to the caller without leaving the buffer in a half-consumed state.

        [Fact]
        public async Task RefillFailureShouldPropagateAndLeaveBufferEmpty()
        {
            var buffer = new GraphBinaryReadBuffer(new ThrowingStream());

            await Assert.ThrowsAsync<IOException>(async () => await buffer.ReadIntValueAsync());

            // A retry hits the same underlying failure, proving no bytes were spuriously committed.
            await Assert.ThrowsAsync<IOException>(async () => await buffer.ReadByteValueAsync());
        }
    }
}
