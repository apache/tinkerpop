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
    /// A serializer that serializes .NET arrays to GraphBinary lists.
    /// </summary>
    /// <typeparam name="TMember">The type of elements in the array.</typeparam>
    public class ArraySerializer<TMember> : SimpleTypeSerializer<TMember[]>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="ArraySerializer{TMember}" /> class.
        /// </summary>
        public ArraySerializer() : base(DataType.List)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(TMember[] value, Stream stream, GraphBinaryWriter writer)
        {
            await writer.WriteValueAsync(value.Length, stream, false).ConfigureAwait(false);
            
            foreach (var item in value)
            {
                await writer.WriteAsync(item, stream).ConfigureAwait(false);
            }
        }
        
        /// <summary>
        /// Currently not supported as GraphBinary has no array data type.
        /// </summary>
        protected override Task<TMember[]> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            throw new NotImplementedException("Reading an array is not supported");
        }
    }
}