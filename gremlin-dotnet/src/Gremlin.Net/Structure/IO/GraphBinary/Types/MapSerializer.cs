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
    /// A generic dictionary serializer.
    /// </summary>
    /// <typeparam name="TKey">The type of keys in the dictionary.</typeparam>
    /// <typeparam name="TValue">The type of values in the dictionary.</typeparam>
    public class MapSerializer<TKey, TValue> : SimpleTypeSerializer<IDictionary<TKey, TValue>>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="MapSerializer{TKey, TValue}" /> class.
        /// </summary>
        public MapSerializer() : base(DataType.Map)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(IDictionary<TKey, TValue> value, Stream stream, GraphBinaryWriter writer)
        {
            await writer.WriteValueAsync(value.Count, stream, false).ConfigureAwait(false);

            foreach (var key in value.Keys)
            {
                await writer.WriteAsync(key, stream).ConfigureAwait(false);
                await writer.WriteAsync(value[key], stream).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        protected override async Task<IDictionary<TKey, TValue>> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var length = await stream.ReadIntAsync().ConfigureAwait(false);
            var result = new Dictionary<TKey, TValue>(length);
            
            for (var i = 0; i < length; i++)
            {
                var key = (TKey) await reader.ReadAsync(stream).ConfigureAwait(false);
                var value = (TValue) await reader.ReadAsync(stream).ConfigureAwait(false);
                result.Add(key, value);
            }

            return result;
        }
    }
}