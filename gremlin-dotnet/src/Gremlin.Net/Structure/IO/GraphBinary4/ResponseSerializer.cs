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
using System.Runtime.CompilerServices;
using System.Threading;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Structure.IO.GraphBinary4
{
    /// <summary>
    ///     Deserializes a streaming GraphBinary 4.0 response, yielding each result object
    ///     as it is deserialized from the stream. After the Marker, reads the status
    ///     footer and throws <see cref="ResponseException"/> if the status code is not 200.
    /// </summary>
    public class ResponseSerializer
    {
        /// <summary>
        ///     Reads a streaming GraphBinary 4.0 response, yielding each result object
        ///     as it is deserialized from the stream. After the Marker, reads the status
        ///     footer and throws <see cref="ResponseException"/> if the status code is not 200.
        /// </summary>
        /// <param name="stream">The stream to read the GraphBinary response from.</param>
        /// <param name="reader">A <see cref="GraphBinaryReader"/> that can be used to read nested values.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>An async enumerable of deserialized result objects.</returns>
        public async IAsyncEnumerable<object> ReadStreamingAsync(Stream stream,
            GraphBinaryReader reader,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // Wrap in BufferedStream for efficient reads (8192 bytes, like Go's bufio).
            // Intentionally not disposed — disposing BufferedStream would also close the
            // underlying stream, which is owned by StreamingResponseContext.
            var buffered = new BufferedStream(stream, 8192);

            // 1. Version byte — validate MSB is set
            var version = await buffered.ReadByteAsync(cancellationToken).ConfigureAwait(false) & 0xFF;
            if (version >> 7 != 1)
            {
                throw new IOException(
                    "The most significant bit should be set according to the format");
            }

            // 2. Bulked flag
            var bulkedByte = await buffered.ReadByteAsync(cancellationToken).ConfigureAwait(false);
            var bulked = bulkedByte == 0x01;

            // 3. Read result data until Marker, yielding each result
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var value = await reader.ReadAsync(buffered, cancellationToken)
                    .ConfigureAwait(false);

                if (value is Marker)
                {
                    break;
                }

                if (bulked)
                {
                    var bulkCount = (long)(await reader.ReadAsync(buffered, cancellationToken)
                        .ConfigureAwait(false))!;
                    yield return new Traverser(value, bulkCount);
                }
                else
                {
                    yield return value!;
                }
            }

            // 4. Status footer
            var statusCode = (int)await reader.ReadNonNullableValueAsync<int>(
                buffered, cancellationToken).ConfigureAwait(false);
            var statusMessage = (string?)await reader.ReadNullableValueAsync<string>(
                buffered, cancellationToken).ConfigureAwait(false);
            var exception = (string?)await reader.ReadNullableValueAsync<string>(
                buffered, cancellationToken).ConfigureAwait(false);

            if (statusCode != 200)
            {
                throw new ResponseException(statusCode, statusMessage, exception);
            }
        }
    }
}
