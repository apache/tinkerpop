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

namespace Gremlin.Net.Structure.IO.GraphBinary4.Types
{
    /// <summary>
    /// A serializer for <see cref="DateTimeOffset"/> values in GraphBinary 4.0.
    /// </summary>
    public class DateTimeSerializer : SimpleTypeSerializer<DateTimeOffset>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="DateTimeSerializer" /> class.
        /// </summary>
        public DateTimeSerializer() : base(DataType.DateTime)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(DateTimeOffset value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await stream.WriteIntAsync(value.Year, cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync((byte)value.Month, cancellationToken).ConfigureAwait(false);
            await stream.WriteByteAsync((byte)value.Day, cancellationToken).ConfigureAwait(false);
            
            // time as Long nanoseconds since midnight
            var timeNanos = (value.Hour * 3600L + value.Minute * 60L + value.Second) * 1_000_000_000L +
                           value.Millisecond * 1_000_000L +
                           (value.Ticks % TimeSpan.TicksPerMillisecond) * 100L;
            await stream.WriteLongAsync(timeNanos, cancellationToken).ConfigureAwait(false);
            
            await stream.WriteIntAsync((int)value.Offset.TotalSeconds, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<DateTimeOffset> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var year = await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false);
            var month = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            var day = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            var timeNanos = await stream.ReadLongAsync(cancellationToken).ConfigureAwait(false);
            var offsetSeconds = await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false);

            // Convert nanoseconds to TimeSpan (100 nanoseconds per tick)
            var timeSpan = TimeSpan.FromTicks(timeNanos / 100);
            var offset = TimeSpan.FromSeconds(offsetSeconds);

            return new DateTimeOffset(year, month, day, timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds, 
                timeSpan.Milliseconds, offset).AddTicks(timeSpan.Ticks % TimeSpan.TicksPerMillisecond);
        }
    }
}
