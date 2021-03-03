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

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    
    /// <summary>
    /// A serializer for byte[].
    /// </summary>
    public class ByteBufferSerializer : SimpleTypeSerializer<byte[]>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ByteBufferSerializer" /> class.
        /// </summary>
        public ByteBufferSerializer() : base(DataType.ByteBuffer)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(byte[] value, Stream stream, GraphBinaryWriter writer)
        {
            await writer.WriteValueAsync(value.Length, stream, false).ConfigureAwait(false);
            await stream.WriteAsync(value, 0, value.Length).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<byte[]> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var length = (int) await reader.ReadValueAsync<int>(stream, false).ConfigureAwait(false);
            var buffer = new byte[length];
            await stream.ReadAsync(buffer, 0, length).ConfigureAwait(false);
            return buffer;
        }
    }
}