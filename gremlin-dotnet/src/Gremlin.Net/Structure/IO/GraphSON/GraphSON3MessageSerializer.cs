﻿#region License

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

namespace Gremlin.Net.Structure.IO.GraphSON
{
    /// <summary>
    ///     Serializes data to and from Gremlin Server in GraphSON3 format.
    /// </summary>
    public class GraphSON3MessageSerializer : GraphSONMessageSerializer
    {
        private const string MimeType = SerializationTokens.GraphSON3MimeType;
        
        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSON3MessageSerializer" /> class with custom serializers.
        /// </summary>
        /// <param name="graphSONReader">The <see cref="GraphSON3Reader"/> used to deserialize from GraphSON.</param>
        /// <param name="graphSONWriter">The <see cref="GraphSON3Writer"/> used to serialize to GraphSON.</param>
        public GraphSON3MessageSerializer(GraphSON3Reader? graphSONReader = null, GraphSON3Writer? graphSONWriter = null)
            : base(MimeType, graphSONReader ?? new GraphSON3Reader(), graphSONWriter ?? new GraphSON3Writer())
        {
        }
    }
}