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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A generic set serializer.
    /// </summary>
    /// <typeparam name="TSet">The type of the set to serialize.</typeparam>
    /// <typeparam name="TMember">The type of elements in the set.</typeparam>
    public class SetSerializer<TSet, TMember> : SimpleTypeSerializer<TSet>
        where TSet : ISet<TMember?>, new()
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="SetSerializer{TSet,TMember}" /> class.
        /// </summary>
        public SetSerializer() : base(DataType.Set)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(TSet value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            var enumerable = (IEnumerable) value;
            var list = enumerable.Cast<object>().ToList();

            await writer.WriteNonNullableValueAsync(list.Count, stream, cancellationToken).ConfigureAwait(false);
            
            foreach (var item in list)
            {
                await writer.WriteAsync(item, stream, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        protected override async Task<TSet> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var length = (int)await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken).ConfigureAwait(false);
            var result = new TSet();
            for (var i = 0; i < length; i++)
            {
                result.Add((TMember?) await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false));
            }

            return result;
        }
    }
}