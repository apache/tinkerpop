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

using System.Collections;
using System.IO;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A serializer for the GraphBinary type BulkSet that gets converted to <typeparamref name="TList"/>.
    /// </summary>
    /// <typeparam name="TList">The type of the list to convert the BulkSet into.</typeparam>
    public class BulkSetSerializer<TList> : SimpleTypeSerializer<TList>
        where TList : IList, new()
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="BulkSetSerializer{TList}" /> class.
        /// </summary>
        public BulkSetSerializer() : base(DataType.BulkSet)
        {
        }

        
        /// <summary>
        /// Currently not supported.
        /// </summary>
        protected override Task WriteValueAsync(TList value, Stream stream, GraphBinaryWriter writer)
        {
            throw new System.NotImplementedException("Writing a BulkSet is not supported");
        }

        /// <inheritdoc />
        protected override async Task<TList> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var length = (int) await reader.ReadValueAsync<int>(stream, false).ConfigureAwait(false);

            var result = new TList();
            for (var i = 0; i < length; i++)
            {
                var value = await reader.ReadAsync(stream).ConfigureAwait(false);
                var bulk = (long) await reader.ReadValueAsync<long>(stream, false).ConfigureAwait(false);
                for (var j = 0; j < bulk; j++)
                {
                    result.Add(value);
                }
            }

            return result;
        }
    }
}