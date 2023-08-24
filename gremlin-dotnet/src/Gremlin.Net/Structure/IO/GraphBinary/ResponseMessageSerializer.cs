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
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Structure.IO.GraphBinary
{
    /// <summary>
    /// Allows to deserialize a <see cref="ResponseMessage{T}"/>.
    /// </summary>
    public class ResponseMessageSerializer
    {
        /// <summary>
        /// Reads a response message from the stream.
        /// </summary>
        /// <param name="stream">The GraphBinary data to parse.</param>
        /// <param name="reader">A <see cref="GraphBinaryReader"/> that can be used to read nested values.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The read response message.</returns>
        public async Task<ResponseMessage<List<object>>> ReadValueAsync(MemoryStream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var version = await stream.ReadByteAsync(cancellationToken).ConfigureAwait(false) & 0xff;

            if (version >> 7 != 1)
            {
                // This is an indication that the response stream was incorrectly built
                // Or the stream offsets are wrong
                throw new IOException("The most significant bit should be set according to the format");
            }

            var requestId =
                (Guid?)await reader.ReadNullableValueAsync<Guid>(stream, cancellationToken).ConfigureAwait(false);
            var code = (ResponseStatusCode)await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken)
                .ConfigureAwait(false);
            var message = (string?)await reader.ReadNullableValueAsync<string>(stream, cancellationToken)
                .ConfigureAwait(false);
            var dictObj = await reader
                .ReadNonNullableValueAsync<Dictionary<string, object>>(stream, cancellationToken).ConfigureAwait(false);
            var attributes = (Dictionary<string, object>)dictObj;

            var status = new ResponseStatus(code, attributes, message);

            var meta = (Dictionary<string, object>)await reader
                .ReadNonNullableValueAsync<Dictionary<string, object>>(stream, cancellationToken).ConfigureAwait(false);
            var data = (List<object>?)await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            var result = new ResponseResult<List<object>>(data, meta);

            return new ResponseMessage<List<object>>(requestId, status, result);
        }
    }
}