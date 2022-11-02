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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Structure.IO.GraphBinary.Types;

namespace Gremlin.Net.Structure.IO.GraphBinary
{
    /// <summary>
    /// Allows to serialize objects to GraphBinary.
    /// </summary>
    public class GraphBinaryWriter
    {
        private const byte ValueFlagNull = 1;
        private const byte ValueFlagNone = 0;

        /// <summary>
        /// A <see cref="byte"/> representing the version of the GraphBinary specification.
        /// </summary>
        public const byte VersionByte = 0x81;

        private static readonly byte[] UnspecifiedNullBytes = {DataType.UnspecifiedNull.TypeCode, 0x01};
        private static readonly byte[] CustomTypeCodeBytes = { DataType.Custom.TypeCode };

        private readonly TypeSerializerRegistry _registry;

        /// <summary>
        /// Initializes a new instance of the <see cref="GraphBinaryWriter" /> class.
        /// </summary>
        /// <param name="registry">The <see cref="TypeSerializerRegistry"/> to use for serialization.</param>
        public GraphBinaryWriter(TypeSerializerRegistry? registry = null)
        {
            _registry = registry ?? TypeSerializerRegistry.Instance;
        }

        /// <summary>
        /// Writes a nullable value without including type information.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public async Task WriteNullableValueAsync(object? value, Stream stream,
            CancellationToken cancellationToken = default)
        {
            if (value == null)
            {
                await WriteValueFlagNullAsync(stream, cancellationToken).ConfigureAwait(false);
                return;
            }
            
            var valueType = value.GetType();
            var serializer = _registry.GetSerializerFor(valueType);
            await serializer.WriteNullableValueAsync(value, stream, this, cancellationToken).ConfigureAwait(false);
        }
        
        /// <summary>
        /// Writes a non-nullable value without including type information.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public async Task WriteNonNullableValueAsync(object value, Stream stream,
            CancellationToken cancellationToken = default)
        {
            if (value == null) throw new IOException($"{nameof(value)} cannot be null");
            var valueType = value.GetType();
            var serializer = _registry.GetSerializerFor(valueType);
            await serializer.WriteNonNullableValueAsync(value, stream, this, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Writes an object in fully-qualified format, containing {type_code}{type_info}{value_flag}{value}.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public async Task WriteAsync(object? value, Stream stream, CancellationToken cancellationToken = default)
        {
            if (value == null)
            {
                await stream.WriteAsync(UnspecifiedNullBytes, cancellationToken).ConfigureAwait(false);
                return;
            }

            var valueType = value.GetType();
            var serializer = _registry.GetSerializerFor(valueType);

            if (serializer is CustomTypeSerializer customTypeSerializer)
            {
                await stream.WriteAsync(CustomTypeCodeBytes, cancellationToken).ConfigureAwait(false);
                await WriteNonNullableValueAsync(customTypeSerializer.TypeName, stream, cancellationToken)
                    .ConfigureAwait(false);
                await customTypeSerializer.WriteAsync(value, stream, this, cancellationToken).ConfigureAwait(false);
                return;
            }

            await stream.WriteByteAsync(serializer.DataType.TypeCode, cancellationToken).ConfigureAwait(false);
            await serializer.WriteAsync(value, stream, this, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Writes a single byte representing the null value_flag.
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public async Task WriteValueFlagNullAsync(Stream stream, CancellationToken cancellationToken = default)
        {
            await stream.WriteByteAsync(ValueFlagNull, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Writes a single byte with value 0, representing an unset value_flag.
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public async Task WriteValueFlagNoneAsync(Stream stream, CancellationToken cancellationToken = default) {
            await stream.WriteByteAsync(ValueFlagNone, cancellationToken).ConfigureAwait(false);
        }

        
    }
}