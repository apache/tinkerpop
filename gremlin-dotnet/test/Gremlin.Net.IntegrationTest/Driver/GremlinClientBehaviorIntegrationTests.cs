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
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.IntegrationTest.Util;
using Gremlin.Net.Structure.IO.GraphBinary;
using Gremlin.Net.Structure.IO.GraphSON;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class GremlinClientBehaviorIntegrationTests
    {
        private static readonly string TestHost = ConfigProvider.Configuration["GremlinSocketServerIpAddress"]!;

        private static readonly SocketServerSettings Settings =
            SocketServerSettings.FromYaml(ConfigProvider.Configuration["GremlinSocketServerConfig"]);

        private static IMessageSerializer Serializer;

        public GremlinClientBehaviorIntegrationTests()
        {
            switch (Settings.Serializer)
            {
                case "GraphSONV2":
                    Serializer = new GraphSON2MessageSerializer();
                    break;
                case "GraphSONV3":
                    Serializer = new GraphSON3MessageSerializer();
                    break;
                case "GraphBinaryV1":
                default:
                    Serializer = new GraphBinaryMessageSerializer();
                    break;
            }
        }

        [Fact(Skip = "WebSocket behavior test using GremlinSocketServer (Phase 2 cleanup)")]
        public async Task ShouldTryCreateNewConnectionIfClosedByServer()
        {
            // Phase 2: test body removed — uses WebSocket-specific GremlinSocketServer
            await Task.CompletedTask;
        }

        [Fact(Skip = "WebSocket behavior test using GremlinSocketServer (Phase 2 cleanup)")]
        public async Task ShouldIncludeUserAgentInHandshakeRequest()
        {
            // Phase 2: test body removed — uses WebSocket-specific GremlinSocketServer
            await Task.CompletedTask;
        }

        [Fact(Skip = "WebSocket behavior test using GremlinSocketServer (Phase 2 cleanup)")]
        public async Task ShouldNotIncludeUserAgentInHandshakeRequestIfDisabled()
        {
            // Phase 2: test body removed — uses WebSocket-specific GremlinSocketServer
            await Task.CompletedTask;
        }

        [Fact(Skip = "WebSocket behavior test using GremlinSocketServer (Phase 2 cleanup)")]
        public async Task ShouldSendPerRequestSettingsToServer()
        {
            // Phase 2: test body removed — uses WebSocket-specific GremlinSocketServer
            await Task.CompletedTask;
        }
    }
}
