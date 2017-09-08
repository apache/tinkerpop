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

namespace Gremlin.Net.Structure.IO.GraphSON
{
    /// <summary>
    /// Handles deserialization of GraphSON3 data.
    /// </summary>
    public class GraphSON3Reader : GraphSONReader
    {
        private static readonly IDictionary<string, IGraphSONDeserializer> GraphSON3SpecificDeserializers =
            new Dictionary<string, IGraphSONDeserializer>
            {
                { "g:List", new ListSerializer() },
                { "g:Set", new SetSerializer() },
                { "g:Map", new MapSerializer() },
                { "g:Path", new Path3Deserializer() }
            };
        
        /// <summary>
        ///     Creates a new instance of <see cref="GraphSON3Reader"/>.
        /// </summary>
        public GraphSON3Reader()
        {
            foreach (var kv in GraphSON3SpecificDeserializers)
            {
                Deserializers[kv.Key] = kv.Value;
            }
        }

        /// <summary>
        ///     Creates a new instance of <see cref="GraphSON3Reader"/>.
        /// </summary>
        /// <param name="deserializerByGraphSONType">
        ///     Overrides <see cref="IGraphSONDeserializer" /> instances by their type identifier.
        /// </param>
        public GraphSON3Reader(IReadOnlyDictionary<string, IGraphSONDeserializer> deserializerByGraphSONType) : this()
        {
            foreach (var deserializerAndGraphSONType in deserializerByGraphSONType)
            {
                Deserializers[deserializerAndGraphSONType.Key] = deserializerAndGraphSONType.Value;   
            }
        }
    }
}