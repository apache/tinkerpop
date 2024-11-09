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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Serializes data to and from Gremlin Server.
    /// </summary>
    public interface IMessageSerializer
    {
        /// <summary>
        ///     Serializes a <see cref="RequestMessage"/>.
        /// </summary>
        /// <param name="requestMessage">The <see cref="RequestMessage"/> to serialize.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The serialized message.</returns>
        Task<byte[]> SerializeMessageAsync(RequestMessage requestMessage,
            CancellationToken cancellationToken = default);

        /// <summary>
        ///     Deserializes a <see cref="ResponseMessage{T}"/> from a byte array.
        /// </summary>
        /// <param name="message">The serialized message to deserialize.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The deserialized <see cref="ResponseMessage{T}"/>.</returns>
        Task<ResponseMessage<List<object>>?> DeserializeMessageAsync(byte[] message,
            CancellationToken cancellationToken = default);
    }
}