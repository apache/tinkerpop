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
using System.IO;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A serializer for the GraphBinary type class that takes <see cref="GremlinType"/> objects as their .NET
    /// representation. 
    /// </summary>
    public class ClassSerializer : SimpleTypeSerializer<GremlinType>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ClassSerializer" /> class.
        /// </summary>
        public ClassSerializer() : base(DataType.Class)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(GremlinType value, Stream stream, GraphBinaryWriter writer)
        {
            await writer.WriteValueAsync(value.Fqcn, stream, false).ConfigureAwait(false);
        }

        
        /// <summary>
        /// Currently not supported.
        /// </summary>
        protected override Task<GremlinType> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            throw new NotImplementedException("Reading a Class is not supported");
        }
    }
}