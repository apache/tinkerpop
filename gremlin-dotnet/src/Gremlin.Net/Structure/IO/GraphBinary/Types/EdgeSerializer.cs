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
    /// An <see cref="Edge"/> serializer.
    /// </summary>
    public class EdgeSerializer : SimpleTypeSerializer<Edge>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="EdgeSerializer" /> class.
        /// </summary>
        public EdgeSerializer() : base(DataType.Edge)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(Edge value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await writer.WriteAsync(value.Id, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(value.Label, stream, cancellationToken).ConfigureAwait(false);

            await writer.WriteAsync(value.InV.Id, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(value.InV.Label, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteAsync(value.OutV.Id, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(value.OutV.Label, stream, cancellationToken).ConfigureAwait(false);

            // Placeholder for the parent vertex
            await writer.WriteAsync(null, stream, cancellationToken).ConfigureAwait(false);

            // placeholder for the properties
            await writer.WriteAsync(null, stream, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<Edge> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var id = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            var label = (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken)
                .ConfigureAwait(false);

            var inV = new Vertex(await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false),
                (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken).ConfigureAwait(false));
            var outV = new Vertex(await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false),
                (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken).ConfigureAwait(false));

            // discard possible parent vertex
            await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);

            var properties = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            var propertiesAsArray = (properties as List<object>)?.ToArray();

            return new Edge(id, outV, label, inV, propertiesAsArray);
        }
    }
}