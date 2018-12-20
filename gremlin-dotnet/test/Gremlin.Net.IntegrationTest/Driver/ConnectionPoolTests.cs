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
using System.Collections.Generic;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.IntegrationTest.Util;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class ConnectionPoolTests
    {
        private readonly RequestMessageProvider _requestMessageProvider = new RequestMessageProvider();
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"];
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);

        private async Task ExecuteMultipleLongRunningRequestsInParallel(IGremlinClient gremlinClient, int nrRequests,
            int requestRunningTimeInMs)
        {
            var tasks = new List<Task>(nrRequests);
            for (var i = 0; i < nrRequests; i++)
            {
                tasks.Add(gremlinClient.SubmitAsync(_requestMessageProvider.GetSleepMessage(requestRunningTimeInMs)));
            }
                
            await Task.WhenAll(tasks);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(8)]
        public void ShouldCreateConfiguredNrConnections(int connectionPoolSize)
        {
            using (var gremlinClient = CreateGremlinClient(connectionPoolSize))
            {
                var nrConnections = gremlinClient.NrConnections;
                Assert.Equal(connectionPoolSize, nrConnections);
            }
        }

        [Fact]
        public async Task ShouldThrowNoConnectionAvailableExceptionWhenNoConnectionIsAvailable()
        {
            const int nrParallelRequests = 2;
            using (var gremlinClient = CreateGremlinClient(connectionPoolSize: 1, maxInProcessPerConnection: 1))
            {
                const int sleepTime = 100;

                await Assert.ThrowsAsync<NoConnectionAvailableException>(() =>
                    ExecuteMultipleLongRunningRequestsInParallel(gremlinClient, nrParallelRequests, sleepTime));
            }
        }

        private static GremlinClient CreateGremlinClient(int connectionPoolSize = 2, int maxInProcessPerConnection = 4)
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            return new GremlinClient(gremlinServer,
                connectionPoolSettings: new ConnectionPoolSettings
                {
                    PoolSize = connectionPoolSize,
                    MaxInProcessPerConnection = maxInProcessPerConnection
                });
        }
    }
}