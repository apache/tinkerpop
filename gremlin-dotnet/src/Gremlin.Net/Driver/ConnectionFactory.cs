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

namespace Gremlin.Net.Driver
{
    internal class ConnectionFactory : IConnectionFactory
    {
        private readonly Action<ClientWebSocketOptions> _webSocketConfiguration;
        private readonly GremlinServer _gremlinServer;
        private readonly string _sessionId;
        private IMessageSerializer _messageSerializer;

        public ConnectionFactory(GremlinServer gremlinServer, IMessageSerializer messageSerializer,
            Action<ClientWebSocketOptions> webSocketConfiguration, string sessionId)
        {
            _gremlinServer = gremlinServer;
            _messageSerializer = messageSerializer;
            _sessionId = sessionId;
            _webSocketConfiguration = webSocketConfiguration;
        }

        public IConnection CreateConnection()
        {
            return new Connection(_gremlinServer.Uri, _gremlinServer.Username, _gremlinServer.Password,
                _messageSerializer, _webSocketConfiguration, _sessionId);
        }
    }
}