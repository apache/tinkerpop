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
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Driver
{
    internal class ProxyClientWebSocket : IClientWebSocket
    {
        private readonly ClientWebSocket _client;

        public ProxyClientWebSocket(ClientWebSocket client)
        {
            _client = client;
        }

        public WebSocketState State => _client.State;

        public ClientWebSocketOptions Options => _client.Options;

        public static IClientWebSocket CreateClientWebSocket()
        {
            return new ProxyClientWebSocket(new ClientWebSocket());
        }

        public Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
        {
            return _client.CloseAsync(closeStatus, statusDescription, cancellationToken);
        }

        public Task ConnectAsync(Uri uri, CancellationToken cancellationToken)
        {
            return _client.ConnectAsync(uri, cancellationToken);
        }

        public void Dispose()
        {
            _client.Dispose();
        }

        public Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            return _client.ReceiveAsync(buffer, cancellationToken);
        }

        public Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            return _client.SendAsync(buffer, messageType, endOfMessage, cancellationToken);
        }

#if NET6_0_OR_GREATER
            public ValueTask SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, WebSocketMessageFlags messageFlags, CancellationToken cancellationToken)
            {
                return _client.SendAsync(buffer, messageType, messageFlags, cancellationToken);
            }
#endif
    }
}
