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

using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A <see cref="Vertex"/> serializer.
    /// </summary>
    public class VertexSerializer : SimpleTypeSerializer<Vertex>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="VertexSerializer" /> class.
        /// </summary>
        public VertexSerializer() : base(DataType.Vertex)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(Vertex value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await writer.WriteAsync(value.Id, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(value.Label, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteAsync(null, stream, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<Vertex> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var id = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            var label = (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken).ConfigureAwait(false);

            var properties = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            var propertiesAsArray = (properties as List<object>)?.ToArray();

            return new Vertex(id, label, propertiesAsArray);
        }
    }
}