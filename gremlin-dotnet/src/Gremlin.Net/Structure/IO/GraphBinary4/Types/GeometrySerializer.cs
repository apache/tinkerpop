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
using Gremlin.Net.Structure.Geo;

namespace Gremlin.Net.Structure.IO.GraphBinary4.Types
{
    /// <summary>
    ///     Serializes a <see cref="Geometry"/> as its SRID followed by its ISO WKB. The end of the WKB is found by
    ///     parsing it, so a value that follows it in the same stream is read next.
    /// </summary>
    /// <remarks>
    ///     The WKB is written big-endian. A value that is read may carry either byte order, because every geometry
    ///     header declares its own.
    /// </remarks>
    public class GeometrySerializer : SimpleTypeSerializer<Geometry>
    {
        private const string IncompleteWkbMessage = "The geometry value ends before its WKB is complete";

        /// <summary>
        ///     Initializes a new instance of the <see cref="GeometrySerializer" /> class.
        /// </summary>
        public GeometrySerializer() : base(DataType.Geometry)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(Geometry value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await stream.WriteIntAsync(value.Srid, cancellationToken).ConfigureAwait(false);
            var wkb = value.ToWkb();
            await stream.WriteAsync(wkb, 0, wkb.Length, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<Geometry> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var srid = await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await WkbReader.ReadAsync(stream, srid, cancellationToken).ConfigureAwait(false);
            }
            catch (EndOfStreamException e)
            {
                throw new IOException(IncompleteWkbMessage, e);
            }
            catch (IOException e)
            {
                // the single-byte stream reader reports the end of the input as a plain IOException
                throw new IOException(IncompleteWkbMessage, e);
            }
            catch (ArgumentException e)
            {
                throw new IOException(e.Message, e);
            }
        }
    }
}
