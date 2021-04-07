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
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A generic list serializer.
    /// </summary>
    /// <typeparam name="TMember">The type of elements in the list.</typeparam>
    public class ListSerializer<TMember> : SimpleTypeSerializer<IList<TMember>>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ListSerializer{TList}" /> class.
        /// </summary>
        public ListSerializer() : base(DataType.List)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(IList<TMember> value, Stream stream, GraphBinaryWriter writer)
        {
            await writer.WriteValueAsync(value.Count, stream, false).ConfigureAwait(false);
            
            foreach (var item in value)
            {
                await writer.WriteAsync(item, stream).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        protected override async Task<IList<TMember>> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var length = (int) await reader.ReadValueAsync<int>(stream, false).ConfigureAwait(false);
            var result = new List<TMember>(length);
            for (var i = 0; i < length; i++)
            {
                result.Add((TMember) await reader.ReadAsync(stream).ConfigureAwait(false));
            }

            return result;
        }
    }
}