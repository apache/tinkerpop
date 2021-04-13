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
        protected override async Task WriteValueAsync(Edge value, Stream stream, GraphBinaryWriter writer)
        {
            await writer.WriteAsync(value.Id, stream).ConfigureAwait(false);
            await writer.WriteValueAsync(value.Label, stream, false).ConfigureAwait(false);
            
            await writer.WriteAsync(value.InV.Id, stream).ConfigureAwait(false);
            await writer.WriteValueAsync(value.InV.Label, stream, false).ConfigureAwait(false);
            await writer.WriteAsync(value.OutV.Id, stream).ConfigureAwait(false);
            await writer.WriteValueAsync(value.OutV.Label, stream, false).ConfigureAwait(false);

            // Placeholder for the parent vertex
            await writer.WriteAsync(null, stream).ConfigureAwait(false);
            
            // placeholder for the properties
            await writer.WriteAsync(null, stream).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<Edge> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var id = await reader.ReadAsync(stream).ConfigureAwait(false);
            var label = (string) await reader.ReadValueAsync<string>(stream, false).ConfigureAwait(false);

            var inV = new Vertex(await reader.ReadAsync(stream).ConfigureAwait(false),
                (string) await reader.ReadValueAsync<string>(stream, false).ConfigureAwait(false));
            var outV = new Vertex(await reader.ReadAsync(stream).ConfigureAwait(false),
                (string) await reader.ReadValueAsync<string>(stream, false).ConfigureAwait(false));
            
            // discard possible parent vertex
            await reader.ReadAsync(stream).ConfigureAwait(false);

            // discard possible properties
            await reader.ReadAsync(stream).ConfigureAwait(false);

            return new Edge(id, outV, label, inV);
        }
    }
}