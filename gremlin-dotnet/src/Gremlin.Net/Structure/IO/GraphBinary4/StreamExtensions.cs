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
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary4
{
    /// <summary>
    ///     Provides extension methods for <see cref="Stream" /> that are mostly useful when implementing GraphBinary
    ///     serializers.
    /// </summary>
    public static class StreamExtensions
    {
        /// <summary>
        ///     Asynchronously writes a <see cref="byte"/> to a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the <see cref="byte"/> to.</param>
        /// <param name="value">The <see cref="byte"/> to write.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        public static async Task WriteByteAsync(this Stream stream, byte value,
            CancellationToken cancellationToken = default)
        {
            await stream.WriteAsync(new[] {value}, 0, 1, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Asynchronously reads a <see cref="byte"/> from a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read <see cref="byte"/>.</returns>
        public static async ValueTask<byte> ReadByteAsync(this Stream stream,
            CancellationToken cancellationToken = default)
        {
            var readBuffer = new byte[1];
            var bytesRead = await stream.ReadAsync(readBuffer.AsMemory(0, 1), cancellationToken)
                .ConfigureAwait(false);
            if (bytesRead == 0)
            {
                throw new IOException("Unexpected end of stream");
            }
            return readBuffer[0];
        }

        /// <summary>
        ///     Asynchronously writes a <see cref="sbyte"/> to a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the <see cref="sbyte"/> to.</param>
        /// <param name="value">The <see cref="sbyte"/> to write.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        public static async Task WriteSByteAsync(this Stream stream, sbyte value,
            CancellationToken cancellationToken = default)
        {
            await stream.WriteByteAsync((byte)value, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Asynchronously reads a <see cref="sbyte"/> from a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read <see cref="sbyte"/>.</returns>
        public static async ValueTask<sbyte> ReadSByteAsync(this Stream stream,
            CancellationToken cancellationToken = default)
        {
            return (sbyte)await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Asynchronously writes an <see cref="int"/> to a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the <see cref="int"/> to.</param>
        /// <param name="value">The <see cref="int"/> to write.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        public static async Task WriteIntAsync(this Stream stream, int value,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes, value);
            await stream.WriteAsync(bytes, 0, 4, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Asynchronously reads an <see cref="int"/> from a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read <see cref="int"/>.</returns>
        public static async ValueTask<int> ReadIntAsync(this Stream stream,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[4];
            await stream.ReadExactlyAsync(bytes, 0, 4, cancellationToken).ConfigureAwait(false);
            return BinaryPrimitives.ReadInt32BigEndian(bytes);
        }

        /// <summary>
        ///     Asynchronously writes a <see cref="long"/> to a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the <see cref="long"/> to.</param>
        /// <param name="value">The <see cref="long"/> to write.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        public static async Task WriteLongAsync(this Stream stream, long value,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(bytes, value);
            await stream.WriteAsync(bytes, 0, 8, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Asynchronously reads a <see cref="long"/> from a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read <see cref="long"/>.</returns>
        public static async ValueTask<long> ReadLongAsync(this Stream stream,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[8];
            await stream.ReadExactlyAsync(bytes, 0, 8, cancellationToken).ConfigureAwait(false);
            return BinaryPrimitives.ReadInt64BigEndian(bytes);
        }

        /// <summary>
        ///     Asynchronously writes a <see cref="float"/> to a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the <see cref="float"/> to.</param>
        /// <param name="value">The <see cref="float"/> to write.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        public static async Task WriteFloatAsync(this Stream stream, float value,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteSingleBigEndian(bytes, value);
            await stream.WriteAsync(bytes, 0, 4, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Asynchronously reads a <see cref="float"/> from a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read <see cref="float"/>.</returns>
        public static async ValueTask<float> ReadFloatAsync(this Stream stream,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[4];
            await stream.ReadExactlyAsync(bytes, 0, 4, cancellationToken).ConfigureAwait(false);
            return BinaryPrimitives.ReadSingleBigEndian(bytes);
        }

        /// <summary>
        ///     Asynchronously writes a <see cref="double"/> to a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the <see cref="double"/> to.</param>
        /// <param name="value">The <see cref="double"/> to write.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        public static async Task WriteDoubleAsync(this Stream stream, double value,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteDoubleBigEndian(bytes, value);
            await stream.WriteAsync(bytes, 0, 8, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Asynchronously reads a <see cref="double"/> from a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read <see cref="double"/>.</returns>
        public static async ValueTask<double> ReadDoubleAsync(this Stream stream,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[8];
            await stream.ReadExactlyAsync(bytes, 0, 8, cancellationToken).ConfigureAwait(false);
            return BinaryPrimitives.ReadDoubleBigEndian(bytes);
        }

        /// <summary>
        ///     Asynchronously writes a <see cref="short"/> to a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the <see cref="short"/> to.</param>
        /// <param name="value">The <see cref="short"/> to write.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        public static async Task WriteShortAsync(this Stream stream, short value,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[2];
            BinaryPrimitives.WriteInt16BigEndian(bytes, value);
            await stream.WriteAsync(bytes, 0, 2, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Asynchronously reads a <see cref="short"/> from a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read <see cref="short"/>.</returns>
        public static async ValueTask<short> ReadShortAsync(this Stream stream,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[2];
            await stream.ReadExactlyAsync(bytes, 0, 2, cancellationToken).ConfigureAwait(false);
            return BinaryPrimitives.ReadInt16BigEndian(bytes);
        }

        /// <summary>
        ///     Asynchronously writes a <see cref="bool"/> to a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the <see cref="bool"/> to.</param>
        /// <param name="value">The <see cref="bool"/> to write.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        public static async Task WriteBoolAsync(this Stream stream, bool value,
            CancellationToken cancellationToken = default)
        {
            await stream.WriteByteAsync((byte)(value ? 1 : 0), cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Asynchronously reads a <see cref="bool"/> from a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read <see cref="bool"/>.</returns>
        public static async ValueTask<bool> ReadBoolAsync(this Stream stream,
            CancellationToken cancellationToken = default)
        {
            var b = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            return b switch
            {
                1 => true,
                0 => false,
                _ => throw new IOException($"Cannot read byte {b} as a boolean.")
            };
        }

        /// <summary>
        ///     Asynchronously writes a <see cref="T:byte[]"/> to a <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the <see cref="T:byte[]"/> to.</param>
        /// <param name="value">The <see cref="T:byte[]"/> to write.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        public static async Task WriteAsync(this Stream stream, byte[] value,
            CancellationToken cancellationToken = default)
        {
            await stream.WriteAsync(value, 0, value.Length, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        ///     Asynchronously reads a <see cref="T:byte[]"/> from a <see cref="Stream"/> into a buffer.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="count">The number of bytes to read.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read <see cref="T:byte[]"/>.</returns>
        public static async ValueTask<byte[]> ReadAsync(this Stream stream, int count,
            CancellationToken cancellationToken = default)
        {
            var buffer = new byte[count];
            await stream.ReadExactlyAsync(buffer, 0, count, cancellationToken).ConfigureAwait(false);
            return buffer;
        }
    }
}
