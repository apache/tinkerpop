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
using System.Diagnostics;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.IntegrationTest.Util;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class MessagesTests
    {
        private readonly RequestMessageProvider _requestMessageProvider = new RequestMessageProvider();
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"];
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);

        [Fact]
        public async Task ShouldUseAliasForTraversalSource()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var aliasTraversalSource = "g2";
                var aliases = new Dictionary<string, string> {{aliasTraversalSource, "g"}};
                var gremlinScript = $"{aliasTraversalSource}.V().count()";
                var requestMsg =
                    RequestMessage.Build(Tokens.OpsEval)
                        .AddArgument(Tokens.ArgsAliases, aliases)
                        .AddArgument(Tokens.ArgsGremlin, gremlinScript)
                        .Create();

                var result = await gremlinClient.SubmitWithSingleResultAsync<long>(requestMsg);

                Assert.InRange(result, 0, long.MaxValue);
            }
        }

        [Fact]
        public async Task ShouldThrowForInvalidOperation()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var ivalidOperationName = "invalid";
                var requestMsg = RequestMessage.Build(ivalidOperationName).Create();

                var thrownException =
                    await Assert.ThrowsAsync<ResponseException>(() => gremlinClient.SubmitAsync<dynamic>(requestMsg));

                Assert.Contains("MalformedRequest", thrownException.Message);
                Assert.Contains(ivalidOperationName, thrownException.Message);
            }
        }

        [Fact]
        public async Task ShouldThrowForInvalidProcessor()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var invalidProcessorName = "invalid";
                var requestMsg = RequestMessage.Build("").Processor(invalidProcessorName).Create();

                var thrownException =
                    await Assert.ThrowsAsync<ResponseException>(() => gremlinClient.SubmitAsync<dynamic>(requestMsg));

                Assert.Contains("InvalidRequestArguments", thrownException.Message);
                Assert.Contains(invalidProcessorName, thrownException.Message);
                Assert.Contains("OpProcessor", thrownException.Message);
            }
        }

        [Fact]
        public async Task ShouldUseSpecifiedEvaluationTimeout()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                const long timeOutInMs = 1L;
                const int scriptSleepTimeInMs = 5000;
                var sleepScript = _requestMessageProvider.GetSleepGremlinScript(scriptSleepTimeInMs);

                var requestMsg =
                    RequestMessage.Build(Tokens.OpsEval)
                        .AddArgument(Tokens.ArgsGremlin, sleepScript)
                        .AddArgument(Tokens.ArgsEvalTimeout, timeOutInMs)
                        .Create();
                var evaluationStopWatch = new Stopwatch();
                evaluationStopWatch.Start();

                var thrownException =
                    await Assert.ThrowsAsync<ResponseException>(() => gremlinClient.SubmitAsync(requestMsg));

                evaluationStopWatch.Stop();
                Assert.Contains("ServerTimeout", thrownException.Message);
                Assert.Contains(timeOutInMs.ToString(), thrownException.Message);
                Assert.True(evaluationStopWatch.ElapsedMilliseconds < scriptSleepTimeInMs);
            }
        }

        [Fact]
        public async Task ShouldThrowForUnsupportedLanguage()
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                var unknownLanguage = "unknown";
                var requestMsg =
                    RequestMessage.Build(Tokens.OpsEval)
                        .AddArgument(Tokens.ArgsGremlin, "1")
                        .AddArgument(Tokens.ArgsLanguage, unknownLanguage)
                        .Create();

                var thrownException =
                    await Assert.ThrowsAsync<ResponseException>(() => gremlinClient.SubmitAsync(requestMsg));

                Assert.Contains("ServerEvaluationError", thrownException.Message);
                Assert.Contains(unknownLanguage, thrownException.Message);
            }
        }
    }
}