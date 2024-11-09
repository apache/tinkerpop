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

using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A serializer for <see cref="BigInteger"/> values.
    /// </summary>
    public class BigIntegerSerializer : SimpleTypeSerializer<BigInteger>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="BigIntegerSerializer" /> class.
        /// </summary>
        public BigIntegerSerializer() : base(DataType.BigInteger)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(BigInteger value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            var bytes = value.ToByteArray().Reverse().ToArray();
            await writer.WriteNonNullableValueAsync(bytes.Length, stream, cancellationToken).ConfigureAwait(false);
            await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<BigInteger> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var length = (int)await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken).ConfigureAwait(false);
            var bytes = await stream.ReadAsync(length, cancellationToken).ConfigureAwait(false);
            return new BigInteger(bytes.Reverse().ToArray());
        }
    }
}