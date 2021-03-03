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
using System.Text;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Structure.IO.GraphBinary
{
    /// <summary>
    ///     Serializes data to and from Gremlin Server in GraphBinary format.
    /// </summary>
    public class GraphBinaryMessageSerializer : IMessageSerializer
    {
        private const string MimeType = SerializationTokens.GraphBinary1MimeType;
        private static readonly byte[] Header = Encoding.UTF8.GetBytes(MimeType);
        
        private readonly GraphBinaryReader _reader;
        private readonly GraphBinaryWriter _writer;
        private readonly RequestMessageSerializer _requestSerializer = new RequestMessageSerializer();
        private readonly ResponseMessageSerializer _responseSerializer = new ResponseMessageSerializer();

        /// <summary>
        /// Initializes a new instance of the <see cref="GraphBinaryMessageSerializer" /> class.
        /// </summary>
        /// <param name="reader">The <see cref="GraphBinaryReader"/> used to deserialize from GraphBinary.</param>
        /// <param name="writer">The <see cref="GraphBinaryWriter"/> used to serialize to GraphBinary.</param>
        public GraphBinaryMessageSerializer(GraphBinaryReader reader = null, GraphBinaryWriter writer = null)
        {
            _reader = reader ?? new GraphBinaryReader();
            _writer = writer ?? new GraphBinaryWriter();
        }

        /// <inheritdoc />
        public async Task<byte[]> SerializeMessageAsync(RequestMessage requestMessage)
        {
            var stream = new MemoryStream();
            await stream.WriteByteAsync((byte) Header.Length).ConfigureAwait(false);
            await stream.WriteAsync(Header).ConfigureAwait(false);
            await _requestSerializer.WriteValueAsync(requestMessage, stream, _writer).ConfigureAwait(false);
            var bytes = stream.ToArray();
            return bytes;
        }

        /// <inheritdoc />
        public async Task<ResponseMessage<List<object>>> DeserializeMessageAsync(byte[] message)
        {
            var stream = new MemoryStream(message);
            return await _responseSerializer.ReadValueAsync(stream, _reader).ConfigureAwait(false);
        }
    }
}