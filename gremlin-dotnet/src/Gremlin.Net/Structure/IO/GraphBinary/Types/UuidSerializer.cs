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

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A serializer that serializes <see cref="Guid"/> values as Uuid in GraphBinary.
    /// </summary>
    public class UuidSerializer : SimpleTypeSerializer<Guid>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="UuidSerializer" /> class.
        /// </summary>
        public UuidSerializer() : base(DataType.Uuid)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(Guid value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            var bytes = value.ToByteArray();
            
            // first 4 bytes in reverse order:
            await stream.WriteByteAsync(bytes[3], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[2], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[1], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[0], cancellationToken).ConfigureAwait(false);
            
            // 2 bytes in reverse order:
            await stream.WriteByteAsync(bytes[5], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[4], cancellationToken).ConfigureAwait(false);
            
            // 3 bytes in reverse order:
            await stream.WriteByteAsync(bytes[7], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[6], cancellationToken).ConfigureAwait(false);
            
            // 3 bytes:
            await stream.WriteByteAsync(bytes[8], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[9], cancellationToken).ConfigureAwait(false);
            
            // last 6 bytes:
            await stream.WriteByteAsync(bytes[10], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[11], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[12], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[13], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[14], cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(bytes[15], cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<Guid> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var bytes = new byte[16];

            // first 4 bytes in reverse order:
            bytes[3] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[2] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[1] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[0] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            
            // 2 bytes in reverse order:
            bytes[5] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[4] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            
            // 2 bytes in reverse order:
            bytes[7] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[6] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            
            // 2 bytes:
            bytes[8] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[9] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            
            // last 6 bytes:
            bytes[10] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[11] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[12] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[13] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[14] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            bytes[15] = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            
            return new Guid(bytes);
        }
    }
}