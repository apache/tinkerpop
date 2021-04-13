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

        private readonly TypeSerializerRegistry _registry = new TypeSerializerRegistry();

        /// <summary>
        /// Writes a value without including type information.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="nullable">Whether or not the value can be null.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public async Task WriteValueAsync(object value, Stream stream, bool nullable)
        {
            if (value == null)
            {
                if (!nullable)
                {
                    throw new IOException("Unexpected null value when nullable is false");
                }

                await WriteValueFlagNullAsync(stream).ConfigureAwait(false);
                return;
            }
            
            var valueType = value.GetType();
            var serializer = _registry.GetSerializerFor(valueType);
            await serializer.WriteValueAsync(value, stream, this, nullable).ConfigureAwait(false);
        }
        
        /// <summary>
        /// Writes an object in fully-qualified format, containing {type_code}{type_info}{value_flag}{value}.
        /// </summary>
        /// <param name="value">The value to write.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public async Task WriteAsync(object value, Stream stream)
        {
            if (value == null)
            {
                await stream.WriteAsync(UnspecifiedNullBytes).ConfigureAwait(false);
                return;
            }

            var valueType = value.GetType();
            var serializer = _registry.GetSerializerFor(valueType);
            await stream.WriteByteAsync(serializer.DataType.TypeCode).ConfigureAwait(false);
            await serializer.WriteAsync(value, stream, this).ConfigureAwait(false);
        }
        
        /// <summary>
        /// Writes a single byte representing the null value_flag.
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public async Task WriteValueFlagNullAsync(Stream stream)
        {
            await stream.WriteByteAsync(ValueFlagNull).ConfigureAwait(false);
        }

        /// <summary>
        /// Writes a single byte with value 0, representing an unset value_flag.
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public async Task WriteValueFlagNoneAsync(Stream stream) {
            await stream.WriteByteAsync(ValueFlagNone).ConfigureAwait(false);
        }

        
    }
}