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
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary4
{
    /// <summary>
    ///     A read-only buffering <see cref="Stream"/> for a single GraphBinary 4.0 response. Exposes
    ///     typed readers for each fixed-width value. When the bytes are already in the internal buffer,
    ///     the reader parses them inline and completes synchronously. Used by a single reader at a
    ///     time; the underlying stream is owned by <c>StreamingResponseContext</c> and is never
    ///     disposed here.
    /// </summary>
    internal sealed class GraphBinaryReadBuffer : Stream
    {
        private const int DefaultBufferSize = 8192;

        private readonly Stream _stream;
        private readonly byte[] _buffer;

        // Unread bytes sit in _buffer[_start.._end). Reads advance _start; when _start == _end the buffer
        // is empty and needs a refill. Invariant: 0 <= _start <= _end.
        private int _start;
        private int _end;

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphBinaryReadBuffer"/> class wrapping the given
        ///     underlying stream, using the default 8192-byte buffer.
        /// </summary>
        /// <param name="stream">The underlying stream to read from. Not owned by this instance.</param>
        internal GraphBinaryReadBuffer(Stream stream) : this(stream, DefaultBufferSize)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphBinaryReadBuffer"/> class wrapping the given
        ///     underlying stream, using a buffer of the given size. Mainly exists so tests can force small
        ///     buffers and frequent refills.
        /// </summary>
        /// <param name="stream">The underlying stream to read from. Not owned by this instance.</param>
        /// <param name="bufferSize">The size of the internal read buffer in bytes.</param>
        internal GraphBinaryReadBuffer(Stream stream, int bufferSize)
        {
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            if (bufferSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(bufferSize));
            }
            _buffer = new byte[bufferSize];
        }

        /// <inheritdoc />
        public override bool CanRead => true;

        /// <inheritdoc />
        public override bool CanWrite => false;

        /// <inheritdoc />
        public override bool CanSeek => false;

        /// <inheritdoc />
        public override long Length => throw new NotSupportedException();

        /// <inheritdoc />
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override void Flush()
        {
        }

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        /// <inheritdoc />
        public override void SetLength(long value) => throw new NotSupportedException();

        /// <inheritdoc />
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        // --- General read side (serve from the internal buffer, refill from the underlying stream) ---
        //
        // Synchronous Read is unsupported. The buffer is always driven asynchronously by
        // ResponseSerializer.ReadStreamingAsync. Throwing here matches Seek/Write and prevents a future
        // caller from accidentally blocking a thread on a refill.

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        /// <inheritdoc />
        public override int Read(Span<byte> buffer) => throw new NotSupportedException();

        /// <inheritdoc />
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count,
            CancellationToken cancellationToken)
        {
            return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        /// <inheritdoc />
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            if (buffer.Length == 0)
            {
                return 0;
            }

            if (_start >= _end)
            {
                await RefillAsync(cancellationToken).ConfigureAwait(false);
                if (_start >= _end)
                {
                    return 0;
                }
            }

            var available = _end - _start;
            var toCopy = Math.Min(available, buffer.Length);
            _buffer.AsSpan(_start, toCopy).CopyTo(buffer.Span);
            _start += toCopy;
            return toCopy;
        }

        // Only called when the buffer is empty, so overwriting _buffer never discards unread data.
        // Indices are updated only after the underlying read returns, so if it throws the buffer stays
        // in its previous state instead of replaying bytes that were already consumed.
        private async ValueTask RefillAsync(CancellationToken cancellationToken)
        {
            var bytesRead = await _stream.ReadAsync(_buffer.AsMemory(0, _buffer.Length), cancellationToken)
                .ConfigureAwait(false);
            _start = 0;
            _end = bytesRead;
        }

        // --- Typed value readers ---
        //
        // Fast path: TryReadSpan(n) parses inline from the internal buffer. Slow path: FillSlowAsync
        // for multi-byte types (throws EndOfStreamException on EOF); ReadByteValueSlowAsync for a
        // single byte (throws IOException on EOF). The EOF split is confined to those two methods.

        /// <summary>
        ///     Tries to serve <paramref name="n"/> bytes directly from the internal buffer with no I/O.
        ///     If <paramref name="n"/> bytes are available, sets <paramref name="span"/> over them, advances
        ///     past them, and returns true. Otherwise returns false and leaves the position unchanged. The
        ///     span points into the internal buffer, so callers must parse it before any <c>await</c>.
        /// </summary>
        // Inlined so a buffered Read*ValueAsync compiles to a single check-and-advance.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryReadSpan(int n, out ReadOnlySpan<byte> span)
        {
            if (_end - _start >= n)
            {
                span = _buffer.AsSpan(_start, n);
                _start += n;
                return true;
            }

            span = default;
            return false;
        }

        /// <summary>
        ///     Reads a single <see cref="byte"/>, returning it inline when already buffered. Throws
        ///     <see cref="IOException"/> on end of stream to match the existing read contract.
        /// </summary>
        internal ValueTask<byte> ReadByteValueAsync(CancellationToken cancellationToken = default)
        {
            if (_end - _start >= 1)
            {
                var value = _buffer[_start];
                _start += 1;
                return new ValueTask<byte>(value);
            }

            return ReadByteValueSlowAsync(cancellationToken);
        }

        // Kept separate from the wider slow path because a byte EOF throws IOException, not
        // EndOfStreamException (see the note above).
        private async ValueTask<byte> ReadByteValueSlowAsync(CancellationToken cancellationToken)
        {
            await RefillAsync(cancellationToken).ConfigureAwait(false);
            if (_start >= _end)
            {
                throw new IOException("Unexpected end of stream");
            }
            var value = _buffer[_start];
            _start += 1;
            return value;
        }

        /// <summary>
        ///     Reads a big-endian <see cref="int"/>, returning it inline when already buffered.
        /// </summary>
        internal ValueTask<int> ReadIntValueAsync(CancellationToken cancellationToken = default)
            => TryReadSpan(4, out var span)
                ? new ValueTask<int>(BinaryPrimitives.ReadInt32BigEndian(span))
                : ReadIntValueSlowAsync(cancellationToken);

        private async ValueTask<int> ReadIntValueSlowAsync(CancellationToken cancellationToken)
            => BinaryPrimitives.ReadInt32BigEndian(await FillSlowAsync(4, cancellationToken).ConfigureAwait(false));

        /// <summary>
        ///     Reads a big-endian <see cref="long"/>, returning it inline when already buffered.
        /// </summary>
        internal ValueTask<long> ReadLongValueAsync(CancellationToken cancellationToken = default)
            => TryReadSpan(8, out var span)
                ? new ValueTask<long>(BinaryPrimitives.ReadInt64BigEndian(span))
                : ReadLongValueSlowAsync(cancellationToken);

        private async ValueTask<long> ReadLongValueSlowAsync(CancellationToken cancellationToken)
            => BinaryPrimitives.ReadInt64BigEndian(await FillSlowAsync(8, cancellationToken).ConfigureAwait(false));

        /// <summary>
        ///     Reads a big-endian <see cref="short"/>, returning it inline when already buffered.
        /// </summary>
        internal ValueTask<short> ReadShortValueAsync(CancellationToken cancellationToken = default)
            => TryReadSpan(2, out var span)
                ? new ValueTask<short>(BinaryPrimitives.ReadInt16BigEndian(span))
                : ReadShortValueSlowAsync(cancellationToken);

        private async ValueTask<short> ReadShortValueSlowAsync(CancellationToken cancellationToken)
            => BinaryPrimitives.ReadInt16BigEndian(await FillSlowAsync(2, cancellationToken).ConfigureAwait(false));

        /// <summary>
        ///     Reads a big-endian <see cref="float"/>, returning it inline when already buffered.
        /// </summary>
        internal ValueTask<float> ReadFloatValueAsync(CancellationToken cancellationToken = default)
            => TryReadSpan(4, out var span)
                ? new ValueTask<float>(BinaryPrimitives.ReadSingleBigEndian(span))
                : ReadFloatValueSlowAsync(cancellationToken);

        private async ValueTask<float> ReadFloatValueSlowAsync(CancellationToken cancellationToken)
            => BinaryPrimitives.ReadSingleBigEndian(await FillSlowAsync(4, cancellationToken).ConfigureAwait(false));

        /// <summary>
        ///     Reads a big-endian <see cref="double"/>, returning it inline when already buffered.
        /// </summary>
        internal ValueTask<double> ReadDoubleValueAsync(CancellationToken cancellationToken = default)
            => TryReadSpan(8, out var span)
                ? new ValueTask<double>(BinaryPrimitives.ReadDoubleBigEndian(span))
                : ReadDoubleValueSlowAsync(cancellationToken);

        private async ValueTask<double> ReadDoubleValueSlowAsync(CancellationToken cancellationToken)
            => BinaryPrimitives.ReadDoubleBigEndian(await FillSlowAsync(8, cancellationToken).ConfigureAwait(false));

        /// <summary>
        ///     Shared slow path for the multi-byte readers. Reads exactly <paramref name="n"/> bytes into a
        ///     small local buffer via <c>ReadExactlyAsync</c> (which goes through this buffer's async read
        ///     path, so the cancellation token flows) and returns it for the caller to parse. Throws
        ///     <see cref="EndOfStreamException"/> on a short stream.
        /// </summary>
        private async ValueTask<byte[]> FillSlowAsync(int n, CancellationToken cancellationToken)
        {
            var bytes = new byte[n];
            await this.ReadExactlyAsync(bytes, 0, n, cancellationToken).ConfigureAwait(false);
            return bytes;
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            // Deliberately does not dispose the underlying stream. That stream is owned by
            // StreamingResponseContext, and there is nothing else to release.
        }

        /// <inheritdoc />
        public override ValueTask DisposeAsync()
        {
            // Deliberately does not dispose the underlying stream. See Dispose(bool).
            return ValueTask.CompletedTask;
        }
    }
}
