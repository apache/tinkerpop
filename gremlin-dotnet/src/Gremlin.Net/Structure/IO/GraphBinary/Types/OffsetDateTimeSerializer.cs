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
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A serializer for the GraphBinary type OffsetDateTime, represented as <see cref="DateTimeOffset"/>
    /// in .NET.
    /// </summary>
    public class OffsetDateTimeSerializer : SimpleTypeSerializer<DateTimeOffset>
    {
        
        /// <summary>
        ///     Initializes a new instance of the <see cref="OffsetDateTimeSerializer" /> class.
        /// </summary>
        public OffsetDateTimeSerializer() : base(DataType.OffsetDateTime)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(DateTimeOffset value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await stream.WriteIntAsync(value.Year, cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(Convert.ToByte(value.Month), cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync(Convert.ToByte(value.Day), cancellationToken).ConfigureAwait(false);
            // Note that nanosecond precisions were added after .NET 7
            // Get the time of day as TimeSpan
            var timeOfDay = value.TimeOfDay; 
            // Convert ticks to nanoseconds (1 tick = 100 nanoseconds)
            var ns = timeOfDay.Ticks * 100;
            await stream.WriteLongAsync(Convert.ToInt64(ns), cancellationToken).ConfigureAwait(false);

            var offset = value.Offset;
            var os = offset.Hours * 60 * 60 + offset.Minutes * 60 + offset.Seconds;
            await stream.WriteIntAsync(os, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<DateTimeOffset> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var year = await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false);
            var month = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            var day = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            var ns = await stream.ReadLongAsync(cancellationToken).ConfigureAwait(false);
            var timeDelta = TimeSpan.FromMilliseconds(ns / 1e6);
            
            var os = await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false);
            var offset = TimeSpan.FromSeconds(os);

            return new DateTimeOffset(year, Convert.ToInt32(month), Convert.ToInt32(day), 0, 0, 0, offset).Add(timeDelta);
        }
    }
}