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
            var longRunningRequestMsg = _requestMessageProvider.GetSleepMessage(requestRunningTimeInMs);
            var tasks = new List<Task>(nrRequests);
            for (var i = 0; i < nrRequests; i++)
                tasks.Add(gremlinClient.SubmitAsync(longRunningRequestMsg));
            await Task.WhenAll(tasks);
        }

        [Fact]
        public async Task ShouldReuseConnectionForSequentialRequests()
        {
            const int minConnectionPoolSize = 1;
            using (var gremlinClient = CreateGremlinClient(minConnectionPoolSize))
            {
                await gremlinClient.SubmitAsync("");
                await gremlinClient.SubmitAsync("");

                var nrConnections = gremlinClient.NrConnections;
                Assert.Equal(1, nrConnections);
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(8)]
        public void ShouldStartWithConfiguredNrMinConnections(int minConnectionPoolSize)
        {
            using (var gremlinClient = CreateGremlinClient(minConnectionPoolSize))
            {
                var nrConnections = gremlinClient.NrConnections;
                Assert.Equal(minConnectionPoolSize, nrConnections);
            }
        }

        [Fact]
        public async Task ShouldExecuteParallelRequestsOnDifferentConnections()
        {
            const int nrParallelRequests = 5;
            using (var gremlinClient = CreateGremlinClient(nrParallelRequests))
            {
                const int sleepTime = 50;

                await ExecuteMultipleLongRunningRequestsInParallel(gremlinClient, nrParallelRequests, sleepTime);

                Assert.Equal(nrParallelRequests, gremlinClient.NrConnections);
            }
        }

        [Fact]
        public async Task ShouldNotCreateMoreThanConfiguredNrMaxConnections()
        {
            const int maxConnectionPoolSize = 1;
            using (var gremlinClient = CreateGremlinClient(maxConnectionPoolSize: maxConnectionPoolSize))
            {
                const int sleepTime = 100;

                await ExecuteMultipleLongRunningRequestsInParallel(gremlinClient, maxConnectionPoolSize + 1, sleepTime);

                Assert.Equal(maxConnectionPoolSize, gremlinClient.NrConnections);
            }
        }

        [Fact]
        public async Task ShouldThrowTimeoutExceptionWhenNoConnectionIsAvailable()
        {
            const int nrParallelRequests = 3;
            const int waitForConnectionTimeoutInMs = 5;
            using (var gremlinClient = CreateGremlinClient(maxConnectionPoolSize: nrParallelRequests - 1,
                waitForConnectionTimeoutInMs: waitForConnectionTimeoutInMs))
            {
                const int sleepTime = 100;

                await Assert.ThrowsAsync<TimeoutException>(() =>
                    ExecuteMultipleLongRunningRequestsInParallel(gremlinClient, nrParallelRequests, sleepTime));
            }
        }

        private static GremlinClient CreateGremlinClient(int minConnectionPoolSize = 0, int maxConnectionPoolSize = 8,
            int waitForConnectionTimeoutInMs = 5000)
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            return new GremlinClient(gremlinServer,
                connectionPoolSettings: new ConnectionPoolSettings
                {
                    MinSize = minConnectionPoolSize,
                    MaxSize = maxConnectionPoolSize,
                    WaitForConnectionTimeout = TimeSpan.FromMilliseconds(waitForConnectionTimeoutInMs)
                });
        }
    }
}