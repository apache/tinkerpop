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
using System.Linq;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.IntegrationTest.Util;
using Gremlin.Net.Structure;
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

        [Fact]
        public async Task ShouldTryCreateNewConnectionIfClosedByServer()
        {
            var sessionId = Guid.NewGuid().ToString();
            var poolSettings = new ConnectionPoolSettings {PoolSize = 1};
            
            var gremlinServer = new GremlinServer(TestHost, Settings.Port);
            var gremlinClient = new GremlinClient(gremlinServer, messageSerializer: Serializer,
                connectionPoolSettings: poolSettings, sessionId: sessionId);

            Assert.Equal(1, gremlinClient.NrConnections);
            
            //Send close request to server, ensure server closes connection
            await Assert.ThrowsAsync<ConnectionClosedException>(async () =>
                await gremlinClient.SubmitWithSingleResultAsync<Vertex>(RequestMessage.Build("1")
                    .OverrideRequestId(Settings.CloseConnectionRequestId).Create()));
            
            //verify that new client reconnects and new requests can be made again
            var response2 = await gremlinClient.SubmitWithSingleResultAsync<Vertex>(RequestMessage.Build("1")
                .OverrideRequestId(Settings.SingleVertexRequestId).Create());
            Assert.NotNull(response2);
            Assert.Equal(1, gremlinClient.NrConnections);
        }
    }
}
