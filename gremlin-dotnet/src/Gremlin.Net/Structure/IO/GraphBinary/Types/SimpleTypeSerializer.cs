﻿#region License

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
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// Base class for serialization of types that don't contain type specific information only {type_code},
    /// {value_flag} and {value}.
    /// </summary>
    /// <typeparam name="T">The supported type.</typeparam>
    public abstract class SimpleTypeSerializer<T> : ITypeSerializer
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="SimpleTypeSerializer{T}" /> class.
        /// </summary>
        protected SimpleTypeSerializer(DataType dataType)
        {
            DataType = dataType;
        }

        /// <inheritdoc />
        public DataType DataType { get; }

        /// <inheritdoc />
        public async Task WriteAsync(object? value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await WriteNullableValueAsync((T?) value, stream, writer, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task WriteNullableValueAsync(object? value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            if (value == null)
            {
                await writer.WriteValueFlagNullAsync(stream, cancellationToken).ConfigureAwait(false);
                return;
            }

            await writer.WriteValueFlagNoneAsync(stream, cancellationToken).ConfigureAwait(false);

            await WriteValueAsync((T) value, stream, writer, cancellationToken).ConfigureAwait(false);
        }
        
        /// <inheritdoc />
        public async Task WriteNonNullableValueAsync(object value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await WriteValueAsync((T) value, stream, writer, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Writes a non-nullable value into a stream.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="writer">A <see cref="GraphBinaryWriter"/>.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        protected abstract Task WriteValueAsync(T value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public async Task<object?> ReadAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            return await ReadNullableValueAsync(stream, reader, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<object?> ReadNullableValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var valueFlag = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            if ((valueFlag & 1) == 1)
            {
                return null;
            }

            return await ReadValueAsync(stream, reader, cancellationToken).ConfigureAwait(false);
        }
        
        /// <inheritdoc />
        public async Task<object> ReadNonNullableValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            return (await ReadValueAsync(stream, reader, cancellationToken).ConfigureAwait(false))!;
        }

        /// <summary>
        /// Reads a non-nullable value according to the type format.
        /// </summary>
        /// <param name="stream">The GraphBinary data to parse.</param>
        /// <param name="reader">A <see cref="GraphBinaryReader"/>.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read value.</returns>
        protected abstract Task<T> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default);
    }
}