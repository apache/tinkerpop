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
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary4.Types
{
    /// <summary>
    /// A <see cref="ProviderDefinedType"/> serializer for the CompositePDT data type.
    /// </summary>
    public class CompositePDTSerializer : SimpleTypeSerializer<ProviderDefinedType>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CompositePDTSerializer"/> class.
        /// </summary>
        public CompositePDTSerializer() : base(DataType.CompositePDT)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(ProviderDefinedType value, Stream stream,
            GraphBinaryWriter writer, CancellationToken cancellationToken = default)
        {
            // Write name as fully-qualified string
            await writer.WriteAsync(value.Name, stream, cancellationToken).ConfigureAwait(false);
            // Write fields as fully-qualified map
            await writer.WriteAsync((IDictionary<string, object?>)new Dictionary<string, object?>(value.Fields),
                stream, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<ProviderDefinedType> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var name = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false) as string;
            if (string.IsNullOrEmpty(name))
                throw new IOException("CompositePDT name cannot be null or empty.");

            var map = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false)
                as IDictionary<object, object?>;

            var fields = new Dictionary<string, object?>();
            if (map != null)
            {
                foreach (var kv in map)
                {
                    fields[(string)kv.Key] = kv.Value;
                }
            }

            return new ProviderDefinedType(name!, fields);
        }
    }
}
