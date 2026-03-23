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
using System.Net.Http;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    /// <summary>
    ///     Integration tests for authentication interceptors against the secure Gremlin Server
    ///     (port 45941, SSL + SimpleAuthenticator).
    /// </summary>
    public class AuthIntegrationTests
    {
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"]!;
        private static readonly int TestSecurePort = Convert.ToInt32(ConfigProvider.Configuration["TestSecureServerPort"]);

        private GremlinClient CreateSecureClient(Func<HttpRequestContext, Task>[]? interceptors = null)
        {
            var gremlinServer = new GremlinServer(TestHost, TestSecurePort, enableSsl: true);
            return new GremlinClient(gremlinServer,
                connectionSettings: new ConnectionSettings { SkipCertificateValidation = true },
                interceptors: interceptors);
        }

        [Fact]
        public async Task ShouldAuthenticateWithBasicAuth()
        {
            // The secure server uses SimpleAuthenticator with credentials: stephen/password
            using var gremlinClient = CreateSecureClient(
                new[] { Auth.BasicAuth("stephen", "password") });

            var response = await gremlinClient.SubmitAsync<long>("g.inject(1).count()");

            Assert.Single(response);
        }

        [Fact]
        public async Task ShouldAuthenticateWithBasicAuthViaDriverRemoteConnection()
        {
            // Test through DriverRemoteConnection + traversal
            using var client = CreateSecureClient(
                new[] { Auth.BasicAuth("stephen", "password") });
            using var remote = new DriverRemoteConnection(client, "gmodern");
            var g = AnonymousTraversalSource.Traversal().With(remote);

            var count = await g.V().Count().Promise(t => t.Next());

            Assert.True(count > 0);
        }

        [Fact]
        public async Task ShouldFailWithWrongCredentials()
        {
            using var gremlinClient = CreateSecureClient(
                new[] { Auth.BasicAuth("stephen", "wrongpassword") });

            // The server returns auth errors as JSON (not GraphBinary), so Connection
            // extracts the message and throws HttpRequestException.
            var ex = await Assert.ThrowsAsync<HttpRequestException>(
                () => gremlinClient.SubmitAsync<long>("g.inject(1).count()"));

            Assert.Contains("incorrect", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public async Task ShouldFailWithNoCredentials()
        {
            using var gremlinClient = CreateSecureClient();

            var ex = await Assert.ThrowsAsync<HttpRequestException>(
                () => gremlinClient.SubmitAsync<long>("g.inject(1).count()"));

            Assert.Contains("credentials", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
