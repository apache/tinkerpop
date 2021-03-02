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
using System.Net.WebSockets;
using System.Threading.Tasks;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.IntegrationTest.Util;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class GremlinClientAuthenticationTests
    {
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"];
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestSecureServerPort"]);
        private readonly RequestMessageProvider _requestMessageProvider = new RequestMessageProvider();

        public static bool IgnoreCertificateValidationLiveDangerouslyWheeeeeeee(
              object sender,
              X509Certificate certificate,
              X509Chain chain,
              SslPolicyErrors sslPolicyErrors)
        {
           return true;
        }

        [Fact]
        public async Task ShouldThrowForMissingCredentials()
        {
            ClientWebSocketOptions optionsSet = null;
            var webSocketConfiguration =
                            new Action<ClientWebSocketOptions>(options =>
                            {
                                options.RemoteCertificateValidationCallback += IgnoreCertificateValidationLiveDangerouslyWheeeeeeee;
                                optionsSet = options;
                            });
            var gremlinServer = new GremlinServer(TestHost, TestPort, enableSsl: true);
            using (var gremlinClient = new GremlinClient(gremlinServer, webSocketConfiguration: webSocketConfiguration))
            {
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                    async () => await gremlinClient.SubmitWithSingleResultAsync<string>(_requestMessageProvider
                        .GetDummyMessage()));

                Assert.Contains("authentication", exception.Message);
                Assert.Contains("credentials", exception.Message);
            }
        }

        [Theory]
        [InlineData("unknownUser", "passwordDoesntMatter")]
        [InlineData("stephen", "wrongPassword")]
        public async Task ShouldThrowForWrongCredentials(string username, string password)
        {
            ClientWebSocketOptions optionsSet = null;
            var webSocketConfiguration =
                            new Action<ClientWebSocketOptions>(options =>
                            {
                                options.RemoteCertificateValidationCallback += IgnoreCertificateValidationLiveDangerouslyWheeeeeeee;
                                optionsSet = options;
                            });
            var gremlinServer = new GremlinServer(TestHost, TestPort, username: username, password: password, enableSsl: true);
            using (var gremlinClient = new GremlinClient(gremlinServer, webSocketConfiguration: webSocketConfiguration))
            {
                var exception = await Assert.ThrowsAsync<ResponseException>(
                    async () => await gremlinClient.SubmitWithSingleResultAsync<string>(_requestMessageProvider
                        .GetDummyMessage()));

                Assert.Contains("Unauthorized", exception.Message);
            }
        }

        [Theory]
        [InlineData("'Hello' + 'World'", "HelloWorld")]
        public async Task ScriptShouldBeEvaluatedAndResultReturnedForCorrectCredentials(string requestMsg,
            string expectedResponse)
        {
            ClientWebSocketOptions optionsSet = null;
            var webSocketConfiguration =
                            new Action<ClientWebSocketOptions>(options =>
                            {
                                options.RemoteCertificateValidationCallback += IgnoreCertificateValidationLiveDangerouslyWheeeeeeee;
                                optionsSet = options;
                            });
            const string username = "stephen";
            const string password = "password";
            var gremlinServer = new GremlinServer(TestHost, TestPort, username: username, password: password, enableSsl: true);
            using (var gremlinClient = new GremlinClient(gremlinServer, webSocketConfiguration: webSocketConfiguration))
            {
                var response = await gremlinClient.SubmitWithSingleResultAsync<string>(requestMsg);

                Assert.Equal(expectedResponse, response);
            }
        }
    }
}