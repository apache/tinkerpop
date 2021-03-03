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
        /// <returns>The read response message.</returns>
        public async Task<ResponseMessage<List<object>>> ReadValueAsync(MemoryStream stream, GraphBinaryReader reader)
        {
            var version = await stream.ReadByteAsync().ConfigureAwait(false) & 0xff;

            if (version >> 7 != 1)
            {
                // This is an indication that the response stream was incorrectly built
                // Or the stream offsets are wrong
                throw new IOException("The most significant bit should be set according to the format");
            }

            var requestId = (Guid?) await reader.ReadValueAsync<Guid>(stream, true).ConfigureAwait(false);
            var code = (ResponseStatusCode) await reader.ReadValueAsync<int>(stream, false).ConfigureAwait(false);
            var message = (string) await reader.ReadValueAsync<string>(stream, true).ConfigureAwait(false);
            var dictObj = await reader
                .ReadValueAsync<Dictionary<string, object>>(stream, false).ConfigureAwait(false);
            var attributes = (Dictionary<string, object>) dictObj;

            var status = new ResponseStatus
            {
                Code = code,
                Message = message,
                Attributes = attributes
            };
            var result = new ResponseResult<List<object>>
            {
                Meta = (Dictionary<string, object>) await reader
                    .ReadValueAsync<Dictionary<string, object>>(stream, false).ConfigureAwait(false),
                Data = (List<object>) await reader.ReadAsync(stream).ConfigureAwait(false)
            };

            return new ResponseMessage<List<object>>
            {
                RequestId = requestId,
                Status = status,
                Result = result
            };
        }
    }
}