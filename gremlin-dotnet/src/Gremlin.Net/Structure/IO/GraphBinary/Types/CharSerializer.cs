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

using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A <see cref="char"/> serializer.
    /// </summary>
    public class CharSerializer : SimpleTypeSerializer<char>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="CharSerializer" /> class.
        /// </summary>
        public CharSerializer() : base(DataType.Char)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(char value, Stream stream, GraphBinaryWriter writer)
        {
            var bytes = Encoding.UTF8.GetBytes(value.ToString());
            await stream.WriteAsync(bytes).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<char> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var firstByte = await stream.ReadByteAsync().ConfigureAwait(false);
            var byteLength = 1;
            // A byte with the first byte ON (10000000) signals that more bytes are needed to represent the UTF-8 char
            if ((firstByte & 0x80) > 0)
            {
                if ((firstByte & 0xf0) == 0xf0)
                { // 0xf0 = 11110000
                    byteLength = 4;
                } else if ((firstByte & 0xe0) == 0xe0)
                { //11100000
                    byteLength = 3;
                } else if ((firstByte & 0xc0) == 0xc0)
                { //11000000
                    byteLength = 2;
                }
            }

            byte[] bytes;
            if (byteLength == 1)
            {
                bytes = new[] {firstByte};
            }
            else
            {
                bytes = new byte[byteLength];
                bytes[0] = firstByte;
                await stream.ReadAsync(bytes, 1, byteLength - 1).ConfigureAwait(false);
            }

            return Encoding.UTF8.GetChars(bytes)[0];
        }
    }
}