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

using System;
using System.Collections.Generic;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    /// <summary>
    /// Handles serialization of GraphSON3 data.
    /// </summary>
    public class GraphSON3Writer : GraphSONWriter
    {
        private static readonly IDictionary<Type, IGraphSONSerializer> GraphSON3SpecificSerializers =
            new Dictionary<Type, IGraphSONSerializer>
            {
                { typeof(IList<object>), new ListSerializer() },
                { typeof(ISet<object>), new SetSerializer() },
                { typeof(IDictionary<object, object>), new MapSerializer() }
            };
        
        /// <summary>
        /// Creates a new instance of <see cref="GraphSON3Writer"/>.
        /// </summary>
        public GraphSON3Writer()
        {
            foreach (var kv in GraphSON3SpecificSerializers)
            {
                Serializers[kv.Key] = kv.Value;
            }
        }

        /// <summary>
        ///     Creates a new instance of <see cref="GraphSON3Writer"/>.
        /// </summary>
        /// <param name="customSerializerByType">
        ///     <see cref="IGraphSONSerializer" /> serializers identified by their
        ///     <see cref="Type" />.
        /// </param>
        public GraphSON3Writer(IReadOnlyDictionary<Type, IGraphSONSerializer> customSerializerByType) : this()
        {
            foreach (var serializerAndType in customSerializerByType)
            {
                Serializers[serializerAndType.Key] = serializerAndType.Value;
            }
        }
    }
}