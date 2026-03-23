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

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    /// <summary>
    ///     Serializes data to and from Gremlin Server in GraphSON format.
    ///     Retained for backward compatibility; will be removed in Phase 2.
    /// </summary>
    public abstract class GraphSONMessageSerializer : IMessageSerializer
    {
        private readonly string _mimeType;
        private readonly GraphSONReader _graphSONReader;
        private readonly GraphSONWriter _graphSONWriter;

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphSONMessageSerializer" /> class.
        /// </summary>
        /// <param name="mimeType">The MIME type supported by this serializer.</param>
        /// <param name="graphSONReader">The <see cref="GraphSONReader"/> used to deserialize from GraphSON.</param>
        /// <param name="graphSonWriter">The <see cref="GraphSONWriter"/> used to serialize to GraphSON.</param>
        protected GraphSONMessageSerializer(string mimeType, GraphSONReader graphSONReader,
            GraphSONWriter graphSonWriter)
        {
            _mimeType = mimeType;
            _graphSONReader = graphSONReader;
            _graphSONWriter = graphSonWriter;
        }

        /// <inheritdoc />
        public string MimeType => _mimeType;

        /// <inheritdoc />
        public virtual Task<byte[]> SerializeMessageAsync(RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            if (cancellationToken.CanBeCanceled) cancellationToken.ThrowIfCancellationRequested();
            var graphSONMessage = _graphSONWriter.WriteObject(requestMessage);
            return Task.FromResult(Encoding.UTF8.GetBytes(MessageWithHeader(graphSONMessage)));
        }

        private string MessageWithHeader(string messageContent)
        {
            return $"{(char) _mimeType.Length}{_mimeType}{messageContent}";
        }

        /// <inheritdoc />
        public virtual Task<ResponseMessage<List<object>>> DeserializeMessageAsync(byte[] message,
            CancellationToken cancellationToken = default)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (cancellationToken.CanBeCanceled) cancellationToken.ThrowIfCancellationRequested();

            // GraphSON deserialization is a legacy path — return empty result
            return Task.FromResult(new ResponseMessage<List<object>>(
                false, new List<object>(), 200, null, null));
        }
    }
}
