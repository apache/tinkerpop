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
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class ClientBehaviorIntegrationTests : IAsyncLifetime
    {
        private static readonly string Host = GetHost();
        private GremlinClient? _client;
        private bool _serverAvailable;

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

        public async Task InitializeAsync()
        {
            var server = new GremlinServer(Host, SocketServerConstants.Port);
            _client = new GremlinClient(server);
            try
            {
                var resultSet = await _client.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
                await resultSet.ToListAsync();
                _serverAvailable = true;
            }
            catch
            {
                _serverAvailable = false;
            }
        }

        public Task DisposeAsync()
        {
            _client?.Dispose();
            return Task.CompletedTask;
        }

        private void SkipIfServerUnavailable()
        {
            if (!_serverAvailable)
                throw new Exception("$XunitDynamicSkip$Socket server not available");
        }

        private GremlinClient CreateClient()
        {
            return new GremlinClient(new GremlinServer(Host, SocketServerConstants.Port));
        }

        [Fact]
        public async Task ShouldReceiveSingleVertex()
        {
            SkipIfServerUnavailable();

            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
            var results = await resultSet.ToListAsync();

            Assert.Single(results);
        }

        [Fact]
        public async Task ShouldHandleServerClosingConnectionBeforeResponse()
        {
            SkipIfServerUnavailable();

            var ex = await Assert.ThrowsAsync<HttpRequestException>(async () =>
            {
                var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinCloseConnection);
                await resultSet.ToListAsync();
            });
            Assert.Contains("error occurred while sending the request", ex.Message);

            // Recovery
            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
            var results = await resultSet.ToListAsync();
            Assert.Single(results);
        }

        [Fact]
        public async Task ShouldHandleServerClosingConnectionAfterResponse()
        {
            SkipIfServerUnavailable();

            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinVertexThenClose);
            var results = await resultSet.ToListAsync();
            Assert.NotEmpty(results);

            await Task.Delay(TimeSpan.FromSeconds(3));

            // Recovery
            resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
            results = await resultSet.ToListAsync();
            Assert.Single(results);
        }

        [Fact]
        public async Task ShouldHandleServerErrorAfterDelay()
        {
            SkipIfServerUnavailable();

            var ex = await Assert.ThrowsAsync<ResponseException>(async () =>
            {
                var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinFailAfterDelay);
                await resultSet.ToListAsync();
            });
            Assert.Equal(500, ex.StatusCode);

            // Recovery
            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
            var results = await resultSet.ToListAsync();
            Assert.Single(results);
        }

        [Fact]
        public async Task ShouldHandlePartialContentClose()
        {
            SkipIfServerUnavailable();

            var ex = await Assert.ThrowsAsync<HttpIOException>(async () =>
            {
                var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinPartialContentClose);
                await resultSet.ToListAsync();
            });
            Assert.Contains("response ended prematurely", ex.Message);

            // Recovery
            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
            var results = await resultSet.ToListAsync();
            Assert.Single(results);
        }

        [Fact]
        public async Task ShouldHandleMalformedResponse()
        {
            SkipIfServerUnavailable();

            // NOTE: the driver surfaces a low-level exception (no Gremlin-aware wrapping,).
            // The exact type is non-deterministic for malformed bytes: either an IOException
            // at the stream layer or a KeyNotFoundException from the GraphBinary deserializer,
            // depending on how the chunk is read.
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinMalformedResponse);
                await resultSet.ToListAsync();
            });
            Assert.True(ex is System.IO.IOException or KeyNotFoundException,
                $"Unexpected exception type: {ex.GetType().FullName}");

            // Recovery
            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
            var results = await resultSet.ToListAsync();
            Assert.Single(results);
        }

        [Fact]
        public async Task ShouldHandleEmptyResponseBody()
        {
            SkipIfServerUnavailable();

            var ex = await Assert.ThrowsAsync<System.IO.IOException>(async () =>
            {
                var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinEmptyBody);
                await resultSet.ToListAsync();
            });
            Assert.Contains("Unexpected end of stream", ex.Message);

            // Recovery
            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
            var results = await resultSet.ToListAsync();
            Assert.Single(results);
        }

        [Fact]
        public async Task ShouldHandleSlowResponse()
        {
            SkipIfServerUnavailable();

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSlowResponse,
                cancellationToken: cts.Token);
            var results = await resultSet.ToListAsync(cts.Token);

            Assert.NotEmpty(results);
        }

        [Fact]
        public async Task ShouldTimeoutWhenServerNeverResponds()
        {
            SkipIfServerUnavailable();

            using var timeoutClient = CreateClient();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

            var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                var resultSet = await timeoutClient.SubmitAsync<dynamic>(
                    SocketServerConstants.GremlinNoResponse, cancellationToken: cts.Token);
                await resultSet.ToListAsync(cts.Token);
            });
            Assert.Contains("operation was canceled", ex.Message);

            // Recovery with main client
            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
            var results = await resultSet.ToListAsync();
            Assert.Single(results);
        }

        [Fact]
        public async Task ShouldTimeoutWhenServerNeverRespondsUsingReadTimeout()
        {
            SkipIfServerUnavailable();

            // A short ReadTimeout must bound the wait for the initial server response even
            // when the caller supplies no cancellation token.
            var settings = new ConnectionSettings { ReadTimeout = TimeSpan.FromSeconds(2) };
            using var timeoutClient = new GremlinClient(
                new GremlinServer(Host, SocketServerConstants.Port), connectionSettings: settings);

            var ex = await Assert.ThrowsAsync<TimeoutException>(async () =>
            {
                var resultSet = await timeoutClient.SubmitAsync<dynamic>(
                    SocketServerConstants.GremlinNoResponse);
                await resultSet.ToListAsync();
            });
            Assert.Contains("waiting for the initial server response", ex.Message);

            // Recovery with main client
            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
            var results = await resultSet.ToListAsync();
            Assert.Single(results);
        }

        [Fact]
        public async Task ShouldHandleAsyncRequestsDuringConnectionClose()
        {
            SkipIfServerUnavailable();

            var task1 = Task.Run(async () =>
            {
                var rs = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinCloseConnection);
                await rs.ToListAsync();
            });
            var task2 = Task.Run(async () =>
            {
                var rs = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinCloseConnection);
                await rs.ToListAsync();
            });

            var results = await Task.WhenAll(
                Task.Run(async () => { try { await task1; return true; } catch { return false; } }),
                Task.Run(async () => { try { await task2; return true; } catch { return false; } }));

            Assert.Contains(false, results);

            // Recovery
            var resultSet = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
            var list = await resultSet.ToListAsync();
            Assert.Single(list);
        }

        [Fact]
        public async Task ShouldHandleConcurrentMixedRequests()
        {
            SkipIfServerUnavailable();

            var goodTasks = Enumerable.Range(0, 5).Select(_ => Task.Run(async () =>
            {
                var rs = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinSingleVertex);
                return await rs.ToListAsync();
            })).ToList();

            var badTasks = Enumerable.Range(0, 5).Select(_ => Task.Run(async () =>
            {
                var rs = await _client!.SubmitAsync<dynamic>(SocketServerConstants.GremlinCloseConnection);
                await rs.ToListAsync();
            })).ToList();

            var goodResults = await Task.WhenAll(goodTasks.Select(async t =>
            {
                try { return await t; }
                catch { return null; }
            }));

            var badResults = await Task.WhenAll(badTasks.Select(async t =>
            {
                try { await t; return false; }
                catch { return true; }
            }));

            Assert.Contains(goodResults, r => r != null && r.Count > 0);
            Assert.All(badResults, failed => Assert.True(failed));
        }
    }
}
