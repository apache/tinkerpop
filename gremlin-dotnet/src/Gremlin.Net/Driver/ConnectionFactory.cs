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
using Gremlin.Net.Structure.IO.GraphSON;

namespace Gremlin.Net.Driver
{
    internal class ConnectionFactory : IConnectionFactory
    {
        private readonly GraphSONReader _graphSONReader;
        private readonly GraphSONWriter _graphSONWriter;
        private readonly Action<ClientWebSocketOptions> _webSocketConfiguration;
        private readonly GremlinServer _gremlinServer;
        private readonly string _mimeType;
        private readonly string _sessionId;

        public ConnectionFactory(GremlinServer gremlinServer, GraphSONReader graphSONReader,
            GraphSONWriter graphSONWriter, string mimeType,
            Action<ClientWebSocketOptions> webSocketConfiguration, string sessionId)
        {
            _gremlinServer = gremlinServer;
            _mimeType = mimeType;
            _sessionId = sessionId;
            _graphSONReader = graphSONReader ?? throw new ArgumentNullException(nameof(graphSONReader));
            _graphSONWriter = graphSONWriter ?? throw new ArgumentNullException(nameof(graphSONWriter));
            _webSocketConfiguration = webSocketConfiguration;
        }

        public IConnection CreateConnection()
        {
            return new Connection(_gremlinServer.Uri, _gremlinServer.Username, _gremlinServer.Password, _graphSONReader,
                                 _graphSONWriter, _mimeType, _webSocketConfiguration, _sessionId);
        }
    }
}