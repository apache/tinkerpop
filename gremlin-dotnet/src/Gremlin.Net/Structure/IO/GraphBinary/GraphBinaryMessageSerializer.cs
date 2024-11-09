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

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
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
        /// <param name="registry">The <see cref="TypeSerializerRegistry"/> to use for serialization.</param>
        public GraphBinaryMessageSerializer(TypeSerializerRegistry? registry = null)
        {
            _reader = new GraphBinaryReader(registry);
            _writer = new GraphBinaryWriter(registry);
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="GraphBinaryMessageSerializer" /> class.
        /// </summary>
        /// <param name="reader">The <see cref="GraphBinaryReader"/> used to deserialize from GraphBinary.</param>
        /// <param name="writer">The <see cref="GraphBinaryWriter"/> used to serialize to GraphBinary.</param>
        [Obsolete("Use the constructor that takes a TypeSerializerRegistry instead.")]
        public GraphBinaryMessageSerializer(GraphBinaryReader reader, GraphBinaryWriter writer)
        {
            _reader = reader;
            _writer = writer;
        }

        /// <inheritdoc />
        public async Task<byte[]> SerializeMessageAsync(RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            using var stream = new MemoryStream();
            await stream.WriteByteAsync((byte) Header.Length, cancellationToken).ConfigureAwait(false);
            await stream.WriteAsync(Header, cancellationToken).ConfigureAwait(false);
            await _requestSerializer.WriteValueAsync(requestMessage, stream, _writer, cancellationToken)
                .ConfigureAwait(false);
            var bytes = stream.ToArray();
            return bytes;
        }

        /// <inheritdoc />
        public async Task<ResponseMessage<List<object>>?> DeserializeMessageAsync(byte[] message,
            CancellationToken cancellationToken = default)
        {
            using var stream = new MemoryStream(message);
            return await _responseSerializer.ReadValueAsync(stream, _reader, cancellationToken).ConfigureAwait(false);
        }
    }
}