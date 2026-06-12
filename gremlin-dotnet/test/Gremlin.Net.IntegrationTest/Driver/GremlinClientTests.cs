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
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.IntegrationTest.Util;
using Gremlin.Net.Structure;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class GremlinClientTests
    {
        private readonly RequestMessageProvider _requestMessageProvider = new();
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"]!;
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);

        [Theory]
        [InlineData("g.inject('justAString')", "justAString")]
        [InlineData("g.inject('HelloWorld')", "HelloWorld")]
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
                var requestMsg = "g.inject(1,2,3,4,5,6,7,8,9,10)";

                var response = await gremlinClient.SubmitAsync<int>(requestMsg);
                var results = await response.ToListAsync();

                Assert.Equal(10, results.Count);
            }
        }

        [Fact]
        public async Task ShouldHandleResponseWithoutContent()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var gremlinScript = "g.V().has('name','unknownTestName')";

                var response =
                    await gremlinClient.SubmitWithSingleResultAsync<object>(gremlinScript);

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
                    await Assert.ThrowsAsync<ResponseException>(async () =>
                    {
                        var resultSet = await gremlinClient.SubmitAsync<object>(requestMsg);
                        await resultSet.ToListAsync();
                    });

                Assert.Equal(typeof(ResponseException), exception.GetType());
                Assert.Contains("Failed to interpret Gremlin query", exception.Message);
            }
        }

        [Fact]
        public async Task ShouldReassembleResponseBatches()
        {
            var expectedResult = new List<int> {1, 2, 3, 4, 5};
            var requestScript = "g.inject(1,2,3,4,5)";
            var requestMessage = RequestMessage.Build(requestScript).Create();
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var response = await gremlinClient.SubmitAsync<int>(requestMessage);

                Assert.Equal(expectedResult, await response.ToListAsync());
            }
        }

        [Fact]
        public async Task ShouldReturnEnumerableResult()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var expectedResult = new List<int> {1, 2, 3, 4, 5};
                var requestMsg = "g.inject(1,2,3,4,5)";

                var response = await gremlinClient.SubmitAsync<int>(requestMsg);

                Assert.Equal(expectedResult, await response.ToListAsync());
            }
        }

        [Fact]
        public async Task ShouldThrowOnExecutionOfSimpleInvalidScript()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var invalidRequestScript = "invalid";

                await Assert.ThrowsAsync<ResponseException>(async () =>
                {
                    var resultSet = await gremlinClient.SubmitAsync<object>(invalidRequestScript);
                    await resultSet.ToListAsync();
                });
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
                var requestMsg = "g.inject(3)";

                var response =
                    await gremlinClient.SubmitWithSingleResultAsync<int>(requestMsg);

                Assert.Equal(3, response);
            }
        }

        [Fact]
        public void ShouldLogWithProvidedLoggerFactory()
        {
            var loggerFactory = Substitute.For<ILoggerFactory>();
            var logger = Substitute.For<ILogger>();
            logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);
            loggerFactory.CreateLogger(Arg.Any<string>()).Returns(logger);
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            
            using var gremlinClient = new GremlinClient(gremlinServer, loggerFactory: loggerFactory);

            logger.VerifyMessageWasLogged(LogLevel.Information, "connections");
        }
        
        [Fact]
        public void ShouldNotLogForDisabledLogLevel()
        {
            var loggerFactory = Substitute.For<ILoggerFactory>();
            var logger = Substitute.For<ILogger>();
            logger.IsEnabled(Arg.Any<LogLevel>()).Returns(false);
            loggerFactory.CreateLogger(Arg.Any<string>()).Returns(logger);
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            
            using var gremlinClient = new GremlinClient(gremlinServer, loggerFactory: loggerFactory);
            
            logger.VerifyNothingWasLogged();
        }

        [Fact]
        public async Task ShouldRoundTripSimplePointPdt()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using var gremlinClient = new GremlinClient(gremlinServer);

            var response = await gremlinClient.SubmitAsync<object>(
                "g.inject(PDT(\"Point\", [\"x\":1, \"y\":2]))");
            var results = await response.ToListAsync();

            Assert.Single(results);
            var pdt = Assert.IsType<ProviderDefinedType>(results[0]);
            Assert.Equal("Point", pdt.Name);
            Assert.Equal(2, pdt.Fields.Count);
            Assert.Equal(1, pdt.Fields["x"]);
            Assert.Equal(2, pdt.Fields["y"]);
        }

        [Fact]
        public async Task ShouldRoundTripNestedPdt()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using var gremlinClient = new GremlinClient(gremlinServer);

            var response = await gremlinClient.SubmitAsync<object>(
                "g.inject(PDT(\"Person\", [\"name\":\"Alice\", \"age\":30, " +
                "\"address\":PDT(\"Address\", [\"street\":\"123 Main St\", \"city\":\"Springfield\", \"zip\":\"12345\"])]))");
            var results = await response.ToListAsync();

            Assert.Single(results);
            var pdt = Assert.IsType<ProviderDefinedType>(results[0]);
            Assert.Equal("Person", pdt.Name);
            Assert.Equal("Alice", pdt.Fields["name"]);
            Assert.Equal(30, pdt.Fields["age"]);

            var address = Assert.IsType<ProviderDefinedType>(pdt.Fields["address"]);
            Assert.Equal("Address", address.Name);
            Assert.Equal("123 Main St", address.Fields["street"]);
            Assert.Equal("Springfield", address.Fields["city"]);
            Assert.Equal("12345", address.Fields["zip"]);
        }

        [Fact]
        public async Task ShouldHandlePdtInCollection()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using var gremlinClient = new GremlinClient(gremlinServer);

            var response = await gremlinClient.SubmitAsync<object>(
                "g.inject([PDT(\"Point\", [\"x\":1, \"y\":2]), PDT(\"Point\", [\"x\":3, \"y\":4])])");
            var results = await response.ToListAsync();

            Assert.Single(results);
            var list = Assert.IsType<List<object>>(results[0]);
            Assert.Equal(2, list.Count);

            var p1 = Assert.IsType<ProviderDefinedType>(list[0]);
            Assert.Equal("Point", p1.Name);
            Assert.Equal(1, p1.Fields["x"]);
            Assert.Equal(2, p1.Fields["y"]);

            var p2 = Assert.IsType<ProviderDefinedType>(list[1]);
            Assert.Equal("Point", p2.Name);
            Assert.Equal(3, p2.Fields["x"]);
            Assert.Equal(4, p2.Fields["y"]);
        }
    }
}
