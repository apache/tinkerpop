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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Structure.IO.GraphBinary
{
    /// <summary>
    /// Allows to serialize a <see cref="RequestMessage"/>.
    /// </summary>
    public class RequestMessageSerializer
    {
        /// <summary>
        /// Write the request message to a stream.
        /// </summary>
        /// <param name="requestMessage">The message to serialize.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="writer">A <see cref="GraphBinaryWriter"/> that can be used to write nested values.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public async Task WriteValueAsync(RequestMessage requestMessage, MemoryStream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await stream.WriteByteAsync(GraphBinaryWriter.VersionByte, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(requestMessage.RequestId, stream, cancellationToken)
                .ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(requestMessage.Operation, stream, cancellationToken)
                .ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(requestMessage.Processor, stream, cancellationToken)
                .ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(requestMessage.Arguments, stream, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}