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

namespace Gremlin.Net.Structure.IO.GraphBinary4.Types
{
    /// <summary>
    /// A generic list serializer for GraphBinary 4.0.
    /// </summary>
    /// <typeparam name="TMember">The type of elements in the list.</typeparam>
    public class ListSerializer<TMember> : SimpleTypeSerializer<IList<TMember?>>
    {
        private const byte ValueFlagBulked = 0x02;
        
        /// <summary>
        ///     Initializes a new instance of the <see cref="ListSerializer{TList}" /> class.
        /// </summary>
        public ListSerializer() : base(DataType.List)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(IList<TMember?> value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await writer.WriteNonNullableValueAsync(value.Count, stream, cancellationToken).ConfigureAwait(false);
            
            foreach (var item in value)
            {
                await writer.WriteAsync(item, stream, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        protected override Task<IList<TMember?>> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            // Default to standard format (valueFlag = 0x00)
            return ReadValueAsync(stream, reader, 0x00, cancellationToken);
        }

        /// <inheritdoc />
        protected override async Task<IList<TMember?>> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            byte valueFlag, CancellationToken cancellationToken = default)
        {
            var length = (int)await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken).ConfigureAwait(false);
            var result = new List<TMember?>();
            var isBulked = (valueFlag & ValueFlagBulked) != 0;

            for (var i = 0; i < length; i++)
            {
                var item = (TMember?)await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
                
                if (isBulked)
                {
                    var bulk = await stream.ReadLongAsync(cancellationToken).ConfigureAwait(false);
                    for (var j = 0; j < bulk; j++)
                    {
                        result.Add(item);
                    }
                }
                else
                {
                    result.Add(item);
                }
            }
            
            return result;
        }
    }
}
