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

namespace Gremlin.Net.Structure.IO.GraphBinary
{
    /// <summary>
    /// Allows to deserialize objects from GraphBinary.
    /// </summary>
    public class GraphBinaryReader
    {
        private readonly TypeSerializerRegistry _registry = new TypeSerializerRegistry();
        
        /// <summary>
        /// Reads only the value for a specific type <typeparamref name="T"/>.
        /// </summary>
        /// <param name="stream">The GraphBinary data to parse.</param>
        /// <param name="nullable">Whether or not the value can be null.</param>
        /// <typeparam name="T">The type of the object to read.</typeparam>
        /// <returns>The read value.</returns>
        public async Task<object> ReadValueAsync<T>(Stream stream, bool nullable)
        {
            var typedSerializer = _registry.GetSerializerFor(typeof(T));
            return await typedSerializer.ReadValueAsync(stream, this, nullable).ConfigureAwait(false);
        }
        
        /// <summary>
        /// Reads the type code, information and value with fully-qualified format.
        /// </summary>
        /// <param name="stream">The GraphBinary data to parse.</param>
        /// <returns>The read value.</returns>
        public async Task<object> ReadAsync(Stream stream)
        {
            var type = DataType.FromTypeCode(await stream.ReadByteAsync().ConfigureAwait(false));

            if (type == DataType.UnspecifiedNull)
            {
                await stream.ReadByteAsync().ConfigureAwait(false); // read value byte to advance the index
                return default; // should be null (TODO?)
            }

            var typedSerializer = _registry.GetSerializerFor(type);
            return await typedSerializer.ReadAsync(stream, this).ConfigureAwait(false);
        }
    }
}