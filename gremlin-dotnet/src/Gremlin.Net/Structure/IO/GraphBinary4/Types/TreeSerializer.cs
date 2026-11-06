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
    /// A <see cref="Tree"/> serializer.
    /// </summary>
    public class TreeSerializer : SimpleTypeSerializer<Tree>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="TreeSerializer" /> class.
        /// </summary>
        public TreeSerializer() : base(DataType.Tree)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(Tree value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            var rootNodes = value.RootNodes();
            await writer.WriteNonNullableValueAsync(rootNodes.Count, stream, cancellationToken).ConfigureAwait(false);

            foreach (var key in rootNodes)
            {
                // key is written fully-qualified (may be null); child is written as a bare value (no
                // type code or value flag, never null).
                await writer.WriteAsync(key, stream, cancellationToken).ConfigureAwait(false);
                await writer.WriteNonNullableValueAsync(value.ChildAt(key), stream, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        protected override async Task<Tree> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var length = await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false);
            var result = new Tree();
            for (var i = 0; i < length; i++)
            {
                var key = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
                var child = (Tree) await reader.ReadNonNullableValueAsync<Tree>(stream, cancellationToken)
                    .ConfigureAwait(false);
                result.GetOrCreateChild(key).AddTree(child);
            }
            return result;
        }
    }
}
