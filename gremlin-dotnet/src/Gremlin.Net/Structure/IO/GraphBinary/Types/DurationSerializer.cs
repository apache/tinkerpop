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
    /// A serializer that serializes <see cref="TimeSpan"/> values as Duration in GraphBinary.
    /// </summary>
    public class DurationSerializer : SimpleTypeSerializer<TimeSpan>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="DurationSerializer" /> class.
        /// </summary>
        public DurationSerializer() : base(DataType.Duration)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(TimeSpan value, Stream stream, GraphBinaryWriter writer)
        {
            await stream.WriteLongAsync((long) value.TotalSeconds).ConfigureAwait(false);
            await stream.WriteIntAsync(value.Milliseconds * 1_000_000).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<TimeSpan> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var seconds = await stream.ReadLongAsync().ConfigureAwait(false);
            var nanoseconds = await stream.ReadIntAsync().ConfigureAwait(false);

            return TimeSpan.FromSeconds(seconds) + TimeSpan.FromMilliseconds(nanoseconds / 1_000_000);
        }
    }
}