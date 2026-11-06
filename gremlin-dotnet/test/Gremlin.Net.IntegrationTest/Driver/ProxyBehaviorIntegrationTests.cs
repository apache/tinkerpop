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
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class ProxyBehaviorIntegrationTests
    {
        private static readonly string Host = GetHost();
        private static readonly string ProxyUrl = GetProxyUrl();

        private static string GetHost()
        {
            var url = Environment.GetEnvironmentVariable("GREMLIN_SOCKET_SERVER_URL");
            if (string.IsNullOrEmpty(url)) return "localhost";
            try
            {
                return new Uri(url).Host;
            }
            catch
            {
                return "localhost";
            }
        }

        private static string GetProxyUrl()
        {
            var url = Environment.GetEnvironmentVariable("GREMLIN_SOCKET_SERVER_PROXY_URL");
            return string.IsNullOrEmpty(url) ? "http://localhost:45944" : url;
        }

        private static async Task ResetProxyAsync(HttpClient httpClient)
        {
            using var response = await httpClient.PostAsync($"{ProxyUrl}/__reset", null);
            response.EnsureSuccessStatusCode();
        }

        private static async Task<string[]> GetRecordedAsync(HttpClient httpClient)
        {
            var json = await httpClient.GetStringAsync($"{ProxyUrl}/__recorded");
            return JsonSerializer.Deserialize<string[]>(json) ?? Array.Empty<string>();
        }

        [Fact]
        public async Task ShouldRouteSocketServerTrafficThroughProxy()
        {
            using var httpClient = new HttpClient();
            await ResetProxyAsync(httpClient);

            var server = new GremlinServer(Host, SocketServerConstants.Port);
            using (var client = new GremlinClient(server,
                       connectionSettings: new ConnectionSettings { Proxy = new WebProxy(ProxyUrl) }))
            {
                var resultSet = await client.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
                var results = await resultSet.ToListAsync();
                Assert.Single(results);
            }

            var recorded = await GetRecordedAsync(httpClient);
            Assert.Contains(recorded, t => t.EndsWith(":45943"));
        }

        [Fact]
        public async Task ShouldNotRecordWhenNoProxy()
        {
            using var httpClient = new HttpClient();
            await ResetProxyAsync(httpClient);

            var server = new GremlinServer(Host, SocketServerConstants.Port);
            using (var client = new GremlinClient(server))
            {
                var resultSet = await client.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
                var results = await resultSet.ToListAsync();
                Assert.Single(results);
            }

            var recorded = await GetRecordedAsync(httpClient);
            Assert.DoesNotContain(recorded, t => t.EndsWith(":45943"));
        }
    }
}
