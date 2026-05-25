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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Serializes data to and from Gremlin Server.
    /// </summary>
    public interface IMessageSerializer
    {
        /// <summary>
        ///     Gets the MIME type produced by this serializer (e.g.
        ///     <c>"application/vnd.graphbinary-v4.0"</c>). Used by the driver to set
        ///     <c>Content-Type</c> and <c>Accept</c> headers automatically.
        /// </summary>
        string MimeType { get; }

        /// <summary>
        ///     Serializes a <see cref="RequestMessage"/>.
        /// </summary>
        /// <param name="requestMessage">The <see cref="RequestMessage"/> to serialize.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The serialized message.</returns>
        Task<byte[]> SerializeMessageAsync(RequestMessage requestMessage,
            CancellationToken cancellationToken = default);

        /// <summary>
        ///     Deserializes result objects from a <see cref="Stream"/>, yielding each
        ///     object as it is deserialized. The stream is expected to contain a
        ///     GraphBinary 4.0 response including the version byte, result data, marker,
        ///     and status footer.
        /// </summary>
        /// <param name="stream">The response stream to deserialize from.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>An async sequence of deserialized result objects.</returns>
        IAsyncEnumerable<object> DeserializeMessageAsync(Stream stream,
            CancellationToken cancellationToken = default);

        /// <summary>
        ///     Sets the <see cref="ProviderDefinedTypeRegistry"/> for automatic hydration
        ///     of provider-defined types during deserialization. The default implementation
        ///     is a no-op for serializers that do not support PDT hydration.
        /// </summary>
        void SetPdtRegistry(ProviderDefinedTypeRegistry pdtRegistry) { }
    }
}