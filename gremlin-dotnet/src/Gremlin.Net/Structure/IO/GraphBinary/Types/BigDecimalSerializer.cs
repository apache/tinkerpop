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
using System.Numerics;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A serializer that serializes <see cref="decimal"/> values as BigDecimal in GraphBinary.
    /// </summary>
    public class BigDecimalSerializer : SimpleTypeSerializer<decimal>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="BigDecimalSerializer" /> class.
        /// </summary>
        public BigDecimalSerializer() : base(DataType.BigDecimal)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(decimal value, Stream stream, GraphBinaryWriter writer)
        {
            var (unscaledValue, scale) = GetUnscaledValueAndScale(value);
            await writer.WriteValueAsync(scale, stream, false).ConfigureAwait(false);
            await writer.WriteValueAsync(unscaledValue, stream, false).ConfigureAwait(false);
        }
        
        private static (BigInteger, int) GetUnscaledValueAndScale(decimal input)
        {
            var parts = decimal.GetBits(input);

            var sign = (parts[3] & 0x80000000) != 0;
            
            var scale = (parts[3] >> 16) & 0x7F;

            var lowBytes = BitConverter.GetBytes(parts[0]);
            var middleBytes = BitConverter.GetBytes(parts[1]);
            var highBytes = BitConverter.GetBytes(parts[2]);
            var valueBytes = new byte[12];
            lowBytes.CopyTo(valueBytes, 0);
            middleBytes.CopyTo(valueBytes, 4);
            highBytes.CopyTo(valueBytes, 8);
            var bigInt = new BigInteger(valueBytes);

            if (sign)
            {
                bigInt = -bigInt;
            }
            
            return (bigInt, scale);
        }

        /// <inheritdoc />
        protected override async Task<decimal> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var scale = (int) await reader.ReadValueAsync<int>(stream, false).ConfigureAwait(false);
            var unscaled = (BigInteger) await reader.ReadValueAsync<BigInteger>(stream, false).ConfigureAwait(false);

            return ConvertScaleAndUnscaledValue(scale, unscaled);
        }

        private static decimal ConvertScaleAndUnscaledValue(int scale, BigInteger unscaledValue)
        {
            return (decimal) unscaledValue * (decimal) Math.Pow(10, -scale);
        }
    }
}