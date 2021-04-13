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
    /// A serializer for the GraphBinary types Date and Timestamp. Both are represented as <see cref="DateTimeOffset"/>
    /// in .NET.
    /// </summary>
    public class DateTimeOffsetSerializer : SimpleTypeSerializer<DateTimeOffset>
    {
        /// <summary>
        /// A serializer for the GraphBinary type Date, represented as <see cref="DateTimeOffset"/> in .NET.
        /// </summary>
        public static readonly DateTimeOffsetSerializer DateSerializer = new DateTimeOffsetSerializer(DataType.Date);

        /// <summary>
        /// A serializer for the GraphBinary type Timestamp, represented as <see cref="DateTimeOffset"/> in .NET.
        /// </summary>
        public static readonly DateTimeOffsetSerializer TimestampSerializer =
            new DateTimeOffsetSerializer(DataType.Timestamp);
        
        private DateTimeOffsetSerializer(DataType dataType) : base(dataType)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(DateTimeOffset value, Stream stream, GraphBinaryWriter writer)
        {
            await stream.WriteLongAsync(value.ToUnixTimeMilliseconds()).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<DateTimeOffset> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(await stream.ReadLongAsync().ConfigureAwait(false));
        }
    }
}