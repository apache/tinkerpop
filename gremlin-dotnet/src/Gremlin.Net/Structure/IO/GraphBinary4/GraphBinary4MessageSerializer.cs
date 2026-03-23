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
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO;

namespace Gremlin.Net.Structure.IO.GraphBinary4
{
    /// <summary>
    ///     Serializes data to and from Gremlin Server in GraphBinary 4.0 format.
    /// </summary>
    public class GraphBinary4MessageSerializer : IMessageSerializer
    {
        private readonly GraphBinaryReader _reader;
        private readonly GraphBinaryWriter _writer;
        private readonly RequestMessageSerializer _requestSerializer = new RequestMessageSerializer();
        private readonly ResponseMessageSerializer _responseSerializer = new ResponseMessageSerializer();

        /// <inheritdoc />
        public string MimeType => SerializationTokens.GraphBinary4MimeType;

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphBinary4MessageSerializer" /> class
        ///     with the default type serializer registry.
        /// </summary>
        public GraphBinary4MessageSerializer()
        {
            _reader = new GraphBinaryReader();
            _writer = new GraphBinaryWriter();
        }

        /// <inheritdoc />
        public async Task<byte[]> SerializeMessageAsync(RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            using var stream = new MemoryStream();
            // No MIME type prefix — HTTP uses Content-Type header
            await _requestSerializer.WriteValueAsync(requestMessage, stream, _writer, cancellationToken)
                .ConfigureAwait(false);
            return stream.ToArray();
        }

        /// <inheritdoc />
        public async Task<ResponseMessage<List<object>>> DeserializeMessageAsync(byte[] message,
            CancellationToken cancellationToken = default)
        {
            using var stream = new MemoryStream(message);
            return await _responseSerializer.ReadValueAsync(stream, _reader, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
