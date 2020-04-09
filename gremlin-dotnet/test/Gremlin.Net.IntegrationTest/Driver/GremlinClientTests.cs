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
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.IntegrationTest.Util;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class GremlinClientTests
    {
        private readonly RequestMessageProvider _requestMessageProvider = new RequestMessageProvider();
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"];
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);

        [Theory]
        [InlineData("'justAString'", "justAString")]
        [InlineData("'Hello' + 'World'", "HelloWorld")]
        public async Task ShouldSendScriptForEvaluationAndReturnCorrectResult(string requestMsg, string expectedResponse)
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var response = await gremlinClient.SubmitWithSingleResultAsync<string>(requestMsg);

                Assert.Equal(expectedResponse, response);
            }
        }

        [Fact]
        public async Task ShouldHandleBigResponse()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var responseMsgSize = 5000;
                var requestMsg = $"'1'*{responseMsgSize}";

                var response = await gremlinClient.SubmitWithSingleResultAsync<string>(requestMsg);

                Assert.Equal(responseMsgSize, response.Length);
            }
        }

        [Fact]
        public async Task ShouldReturnResultWithoutDeserializingItForJTokenType()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var gremlinScript = "'someString'";
                
                var response = await gremlinClient.SubmitWithSingleResultAsync<JToken>(gremlinScript);

                //Expected:
                /* {
                  "@type": "g:List",
                  "@value": [
                    "someString"
                  ]
                }*/

                Assert.IsType<JObject>(response);
                Assert.Equal("g:List", response["@type"]);

                var jArray = response["@value"] as JArray;
                Assert.NotNull(jArray);
                Assert.Equal(1, jArray.Count);
                Assert.Equal("someString", (jArray[0] as JValue)?.Value);
            }
        }

        [Fact]
        public async Task ShouldHandleResponseWithoutContent()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var gremlinScript = "g.V().has(propertyKey, propertyValue);";
                var bindings = new Dictionary<string, object>
                {
                    {"propertyKey", "name"},
                    {"propertyValue", "unknownTestName"}
                };

                var response =
                    await gremlinClient.SubmitWithSingleResultAsync<object>(gremlinScript, bindings);

                Assert.Null(response);
            }
        }

        [Fact]
        public async Task ShouldThrowExceptionForInvalidScript()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var requestMsg = "invalid";

                var exception =
                    await Assert.ThrowsAsync<ResponseException>(() => gremlinClient.SubmitAsync(requestMsg));

                Assert.Equal(typeof(ResponseException), exception.GetType());
                Assert.Contains($"ScriptEvaluationError: No such property: {requestMsg}",
                    exception.Message);
            }
        }

        [Fact]
        public async Task ShouldReassembleResponseBatches()
        {
            const int batchSize = 2;
            var expectedResult = new List<int> {1, 2, 3, 4, 5};
            var requestScript = $"{nameof(expectedResult)}";
            var bindings = new Dictionary<string, object> {{nameof(expectedResult), expectedResult}};
            var requestMessage =
                RequestMessage.Build(Tokens.OpsEval)
                    .AddArgument(Tokens.ArgsBatchSize, batchSize)
                    .AddArgument(Tokens.ArgsGremlin, requestScript)
                    .AddArgument(Tokens.ArgsBindings, bindings)
                    .Create();
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var response = await gremlinClient.SubmitAsync<int>(requestMessage);

                Assert.Equal(expectedResult, response);
            }
        }

        [Fact]
        public async Task ShouldCorrectlyAssignResponsesToRequests()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var sleepTime = 100;
                var expectedFirstResult = 1;
                var gremlinScript = _requestMessageProvider.GetSleepGremlinScript(sleepTime);
                gremlinScript += $"{expectedFirstResult}";
                var firstRequestMsg = RequestMessage.Build(Tokens.OpsEval)
                    .AddArgument(Tokens.ArgsGremlin, gremlinScript).Create();
                var expectedSecondResponse = 2;
                var secondScript = $"{expectedSecondResponse}";

                var firstResponseTask = gremlinClient.SubmitWithSingleResultAsync<int>(firstRequestMsg);
                var secondResponseTask = gremlinClient.SubmitWithSingleResultAsync<int>(secondScript);

                var secondResponse = await secondResponseTask;
                Assert.Equal(expectedSecondResponse, secondResponse);
                var firstResponse = await firstResponseTask;
                Assert.Equal(expectedFirstResult, firstResponse);
            }
        }

        [Fact]
        public async Task ShouldReturnEnumerableResult()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var expectedResult = new List<int> {1, 2, 3, 4, 5};
                var requestMsg = $"{nameof(expectedResult)}";
                var bindings = new Dictionary<string, object> {{nameof(expectedResult), expectedResult}};

                var response = await gremlinClient.SubmitAsync<int>(requestMsg, bindings);

                Assert.Equal(expectedResult, response);
            }
        }

        [Fact]
        public async Task ShouldThrowOnExecutionOfSimpleInvalidScript()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var invalidRequestScript = "invalid";

                await Assert.ThrowsAsync<ResponseException>(() => gremlinClient.SubmitAsync(invalidRequestScript));
            }
        }

        [Fact]
        public async Task ShouldHandleSimpleScriptWithoutErrors()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var requestMsg = _requestMessageProvider.GetDummyMessage();

                await gremlinClient.SubmitAsync(requestMsg);
            }
        }

        [Fact]
        public async Task ShouldUseBindingsForScript()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var requestMsg = "a + b";
                var a = 1;
                var b = 2;
                var bindings = new Dictionary<string, object> {{"a", a}, {"b", b}};

                var response =
                    await gremlinClient.SubmitWithSingleResultAsync<int>(requestMsg, bindings);

                Assert.Equal(a + b, response);
            }
        }

        [Fact]
        public async Task ShouldConfigureWebSocketOptionsAsSpecified()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            ClientWebSocketOptions optionsSet = null;
            var expectedKeepAliveInterval = TimeSpan.FromMilliseconds(11);
            var webSocketConfiguration =
                new Action<ClientWebSocketOptions>(options =>
                {
                    options.UseDefaultCredentials = false;
                    options.KeepAliveInterval = expectedKeepAliveInterval;
                    optionsSet = options;
                });
            using (var gremlinClient = new GremlinClient(gremlinServer, webSocketConfiguration: webSocketConfiguration))
            {
                // send dummy message to create at least one connection
                await gremlinClient.SubmitAsync(_requestMessageProvider.GetDummyMessage());
                
                Assert.NotNull(optionsSet);
                Assert.False(optionsSet.UseDefaultCredentials);
                Assert.Equal(expectedKeepAliveInterval, optionsSet.KeepAliveInterval);
            }
        }

        [Fact]
        public async Task ShouldSaveVariableBetweenRequestsInSession()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer, sessionId: Guid.NewGuid().ToString()))
            {
                await gremlinClient.SubmitAsync<int>("x = 1");

                var expectedResult = new List<int> {3};
                var response = await gremlinClient.SubmitAsync<int>("x + 2");

                Assert.Equal(expectedResult, response);
            }
        }
    }
}