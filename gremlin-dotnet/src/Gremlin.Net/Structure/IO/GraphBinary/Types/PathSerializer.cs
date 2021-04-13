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
    /// A <see cref="Path"/> serializer.
    /// </summary>
    public class PathSerializer : SimpleTypeSerializer<Path>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="PathSerializer" /> class.
        /// </summary>
        public PathSerializer() : base(DataType.Path)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(Path value, Stream stream, GraphBinaryWriter writer)
        {
            await writer.WriteAsync(value.Labels, stream).ConfigureAwait(false);
            await writer.WriteAsync(value.Objects, stream).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<Path> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var readLabelObjects = (List<object>) await reader.ReadAsync(stream).ConfigureAwait(false);
            var labels = new List<ISet<string>>();
            foreach (var labelObjectList in readLabelObjects)
            {
                var labelSet = new HashSet<string>();
                foreach (var labelObj in (HashSet<object>) labelObjectList)
                {
                    labelSet.Add((string) labelObj);
                }
                labels.Add(labelSet);
            }
            
            var objects = (List<object>) await reader.ReadAsync(stream).ConfigureAwait(false);
            
            return new Path(labels, objects);
        }
    }
}