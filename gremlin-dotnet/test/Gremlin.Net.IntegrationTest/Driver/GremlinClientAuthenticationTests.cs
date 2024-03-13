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
using System.Net.WebSockets;
using System.Threading.Tasks;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class GremlinClientAuthenticationTests
    {
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"];
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestSecureServerPort"]);

        public static bool IgnoreCertificateValidationLiveDangerouslyWheeeeeeee(
              object sender,
              X509Certificate certificate,
              X509Chain chain,
              SslPolicyErrors sslPolicyErrors)
        {
           return true;
        }

        [Fact]
        public void ShouldThrowForMissingCredentials()
        {
            var webSocketConfiguration =
                            new Action<ClientWebSocketOptions>(options =>
                            {
                                options.RemoteCertificateValidationCallback += IgnoreCertificateValidationLiveDangerouslyWheeeeeeee;
                            });
            var gremlinServer = new GremlinServer(TestHost, TestPort, enableSsl: true);
            var aggregateException = Assert.Throws<AggregateException>(() =>
                new GremlinClient(gremlinServer, webSocketConfiguration: webSocketConfiguration));

            var innerException = aggregateException.InnerException;
            Assert.Equal(typeof(InvalidOperationException), innerException!.GetType());
            Assert.Contains("authentication", innerException.Message);
            Assert.Contains("credentials", innerException.Message);
        }

        [Theory]
        [InlineData("unknownUser", "passwordDoesntMatter")]
        [InlineData("stephen", "wrongPassword")]
        public void ShouldThrowForWrongCredentials(string username, string password)
        {
            var webSocketConfiguration =
                            new Action<ClientWebSocketOptions>(options =>
                            {
                                options.RemoteCertificateValidationCallback += IgnoreCertificateValidationLiveDangerouslyWheeeeeeee;
                            });
            var gremlinServer = new GremlinServer(TestHost, TestPort, username: username, password: password, enableSsl: true);
            var aggregateException = Assert.Throws<AggregateException>(() =>
                new GremlinClient(gremlinServer, webSocketConfiguration: webSocketConfiguration));

            var innerException = aggregateException.InnerException;
            Assert.Equal(typeof(ResponseException), innerException!.GetType());

            Assert.Contains("Unauthorized", innerException.Message);
        }

        [Theory]
        [InlineData("'Hello' + 'World'", "HelloWorld")]
        public async Task ScriptShouldBeEvaluatedAndResultReturnedForCorrectCredentials(string requestMsg,
            string expectedResponse)
        {
            var webSocketConfiguration =
                            new Action<ClientWebSocketOptions>(options =>
                            {
                                options.RemoteCertificateValidationCallback += IgnoreCertificateValidationLiveDangerouslyWheeeeeeee;
                            });
            const string username = "stephen";
            const string password = "password";
            var gremlinServer = new GremlinServer(TestHost, TestPort, username: username, password: password, enableSsl: true);
            using var gremlinClient = new GremlinClient(gremlinServer, webSocketConfiguration: webSocketConfiguration);

            var response = await gremlinClient.SubmitWithSingleResultAsync<string>(requestMsg);

            Assert.Equal(expectedResponse, response);
        }

        [Fact]
        public async Task ExecutingRequestsInParallelOverSameConnectionShouldWorkWithAuthentication()
        {
            var webSocketConfiguration =
                new Action<ClientWebSocketOptions>(options =>
                {
                    options.RemoteCertificateValidationCallback += IgnoreCertificateValidationLiveDangerouslyWheeeeeeee;
                });
            const string username = "stephen";
            const string password = "password";
            const int nrOfParallelRequests = 10;
            var gremlinServer = new GremlinServer(TestHost, TestPort, username: username, password: password, enableSsl: true);
            var connectionPoolSettings = new ConnectionPoolSettings
                { PoolSize = 1, MaxInProcessPerConnection = nrOfParallelRequests };

            using var gremlinClient = new GremlinClient(gremlinServer, webSocketConfiguration: webSocketConfiguration,
                connectionPoolSettings: connectionPoolSettings);
            var tasks = new List<Task>(nrOfParallelRequests);
            for (var i = 0; i < nrOfParallelRequests; i++)
            {
                tasks.Add(gremlinClient.SubmitWithSingleResultAsync<string>("''"));
            }

            await Task.WhenAll(tasks);
        }
    }
}