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
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary4.Types
{
    /// <summary>
    /// A <see cref="PrimitiveProviderDefinedType"/> serializer for the PrimitivePDT data type.
    /// Wire format: two fully-qualified Strings {name}{value}.
    /// </summary>
    public class PrimitivePDTSerializer : SimpleTypeSerializer<PrimitiveProviderDefinedType>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PrimitivePDTSerializer"/> class.
        /// </summary>
        public PrimitivePDTSerializer() : base(DataType.PrimitivePDT)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(PrimitiveProviderDefinedType value, Stream stream,
            GraphBinaryWriter writer, CancellationToken cancellationToken = default)
        {
            await writer.WriteAsync(value.Name, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteAsync(value.Value, stream, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<PrimitiveProviderDefinedType> ReadValueAsync(Stream stream,
            GraphBinaryReader reader, CancellationToken cancellationToken = default)
        {
            var name = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false) as string;
            if (string.IsNullOrEmpty(name))
                throw new IOException("PrimitivePDT name cannot be null or empty.");

            var value = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false) as string;
            if (value == null)
                throw new IOException("PrimitivePDT value cannot be null.");

            return new PrimitiveProviderDefinedType(name!, value);
        }
    }
}
