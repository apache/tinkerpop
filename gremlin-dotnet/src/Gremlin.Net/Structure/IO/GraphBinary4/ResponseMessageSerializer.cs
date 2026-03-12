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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Structure.IO.GraphBinary4
{
    /// <summary>
    ///     Deserializes a <see cref="ResponseMessage{T}"/> from the GraphBinary 4.0 streaming format.
    /// </summary>
    public class ResponseMessageSerializer
    {
        /// <summary>
        ///     Reads a response message from the stream in the 4.0 format:
        ///     version byte + bulked flag + result data + marker + status footer.
        /// </summary>
        /// <param name="stream">The GraphBinary data to parse.</param>
        /// <param name="reader">A <see cref="GraphBinaryReader"/> that can be used to read nested values.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read response message.</returns>
        public async Task<ResponseMessage<List<object>>> ReadValueAsync(MemoryStream stream,
            GraphBinaryReader reader, CancellationToken cancellationToken = default)
        {
            // 1. Version byte — validate MSB is set
            var version = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false) & 0xFF;
            if (version >> 7 != 1)
            {
                throw new IOException("The most significant bit should be set according to the format");
            }

            // 2. Bulked flag
            var bulkedByte = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            var bulked = bulkedByte == 0x01;

            // 3. Read result data until Marker
            var results = new List<object>();
            while (true)
            {
                var value = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);

                if (value is Marker)
                {
                    break;
                }

                if (bulked)
                {
                    // Read the fully-qualified Long bulk count following the value
                    var bulkCount = (long)(await reader.ReadAsync(stream, cancellationToken)
                        .ConfigureAwait(false))!;
                    results.Add(new Traverser(value, bulkCount));
                }
                else
                {
                    results.Add(value!);
                }
            }

            // 4. Status footer: status code (Int value)
            var statusCode = (int)await reader.ReadNonNullableValueAsync<int>(
                stream, cancellationToken).ConfigureAwait(false);

            // 5. Nullable status message (value_flag + String value)
            var statusMessage = (string?)await reader.ReadNullableValueAsync<string>(
                stream, cancellationToken).ConfigureAwait(false);

            // 6. Nullable exception (value_flag + String value)
            var exception = (string?)await reader.ReadNullableValueAsync<string>(
                stream, cancellationToken).ConfigureAwait(false);

            if (statusCode != 200)
            {
                throw new ResponseException(statusCode, statusMessage, exception);
            }

            return new ResponseMessage<List<object>>(bulked, results, statusCode,
                statusMessage, exception);
        }
    }
}
