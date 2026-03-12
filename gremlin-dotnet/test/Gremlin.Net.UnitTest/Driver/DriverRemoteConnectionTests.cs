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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using NSubstitute;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class DriverRemoteConnectionTests
    {
        [Fact]
        public void ShouldDisposeProvidedGremlinClientOnDispose()
        {
            var gremlinClient = Substitute.For<IGremlinClient>();
            var driverRemoteConnection = new DriverRemoteConnection(gremlinClient);

            driverRemoteConnection.Dispose();

            gremlinClient.Received().Dispose();
        }

        [Fact]
        public void ShouldThrowWhenGivenNullAsGremlinClient()
        {
            Assert.Throws<ArgumentNullException>(() => new DriverRemoteConnection(null!));
        }

        [Fact]
        public async Task ShouldBuildRequestWithGremlinString()
        {
            RequestMessage? capturedRequest = null;
            var client = CreateCapturingClient(msg => capturedRequest = msg);
            var connection = new DriverRemoteConnection(client, "g");

            var gl = new GremlinLang();
            gl.AddStep("V", Array.Empty<object>());

            await connection.SubmitAsync<object, object>(gl);

            Assert.NotNull(capturedRequest);
            Assert.Contains(".V()", capturedRequest!.Gremlin);
        }

        [Fact]
        public async Task ShouldBuildRequestWithTraversalSource()
        {
            RequestMessage? capturedRequest = null;
            var client = CreateCapturingClient(msg => capturedRequest = msg);
            var connection = new DriverRemoteConnection(client, "mySource");

            var gl = new GremlinLang();
            gl.AddStep("V", Array.Empty<object>());

            await connection.SubmitAsync<object, object>(gl);

            Assert.NotNull(capturedRequest);
            Assert.Equal("mySource", capturedRequest!.Fields[Tokens.ArgsG]);
        }

        [Fact]
        public async Task ShouldBuildRequestWithDefaultLanguage()
        {
            RequestMessage? capturedRequest = null;
            var client = CreateCapturingClient(msg => capturedRequest = msg);
            var connection = new DriverRemoteConnection(client, "g");

            var gl = new GremlinLang();
            gl.AddStep("V", Array.Empty<object>());

            await connection.SubmitAsync<object, object>(gl);

            Assert.NotNull(capturedRequest);
            Assert.Equal("gremlin-lang", capturedRequest!.Fields[Tokens.ArgsLanguage]);
        }

        [Fact]
        public async Task ShouldBuildRequestWithBindings()
        {
            RequestMessage? capturedRequest = null;
            var client = CreateCapturingClient(msg => capturedRequest = msg);
            var connection = new DriverRemoteConnection(client, "g");

            var gl = new GremlinLang();
            gl.Parameters["_0"] = 42;
            gl.AddStep("V", Array.Empty<object>());

            await connection.SubmitAsync<object, object>(gl);

            Assert.NotNull(capturedRequest);
            var bindings = (Dictionary<string, object>)capturedRequest!.Fields[Tokens.ArgsBindings];
            Assert.Equal(42, bindings["_0"]);
        }

        [Fact]
        public async Task ShouldDefaultBulkResultsToTrue()
        {
            RequestMessage? capturedRequest = null;
            var client = CreateCapturingClient(msg => capturedRequest = msg);
            var connection = new DriverRemoteConnection(client, "g");

            var gl = new GremlinLang();
            gl.AddStep("V", Array.Empty<object>());

            await connection.SubmitAsync<object, object>(gl);

            Assert.NotNull(capturedRequest);
            Assert.Equal("true", capturedRequest!.Fields[Tokens.ArgsBulkResults]);
        }

        [Fact]
        public async Task ShouldExtractAllowedOptionsStrategyKeys()
        {
            RequestMessage? capturedRequest = null;
            var client = CreateCapturingClient(msg => capturedRequest = msg);
            var connection = new DriverRemoteConnection(client, "g");

            var gl = new GremlinLang();
            gl.AddStep("V", Array.Empty<object>());
            gl.OptionsStrategies.Add(new OptionsStrategy(new Dictionary<string, object>
            {
                { Tokens.ArgsEvalTimeout, 5000L },
                { Tokens.ArgsBatchSize, 100 }
            }));

            await connection.SubmitAsync<object, object>(gl);

            Assert.NotNull(capturedRequest);
            Assert.Equal(5000L, capturedRequest!.Fields[Tokens.ArgsEvalTimeout]);
            Assert.Equal(100, capturedRequest.Fields[Tokens.ArgsBatchSize]);
        }

        [Fact]
        public async Task ShouldNotExtractDisallowedOptionsStrategyKeys()
        {
            RequestMessage? capturedRequest = null;
            var client = CreateCapturingClient(msg => capturedRequest = msg);
            var connection = new DriverRemoteConnection(client, "g");

            var gl = new GremlinLang();
            gl.AddStep("V", Array.Empty<object>());
            gl.OptionsStrategies.Add(new OptionsStrategy(new Dictionary<string, object>
            {
                { "someCustomKey", "someValue" }
            }));

            await connection.SubmitAsync<object, object>(gl);

            Assert.NotNull(capturedRequest);
            Assert.False(capturedRequest!.Fields.ContainsKey("someCustomKey"));
        }

        [Fact]
        public async Task ShouldRespectPerRequestBulkResults()
        {
            RequestMessage? capturedRequest = null;
            var client = CreateCapturingClient(msg => capturedRequest = msg);
            var connection = new DriverRemoteConnection(client, "g");

            var gl = new GremlinLang();
            gl.AddStep("V", Array.Empty<object>());
            gl.OptionsStrategies.Add(new OptionsStrategy(new Dictionary<string, object>
            {
                { Tokens.ArgsBulkResults, "false" }
            }));

            await connection.SubmitAsync<object, object>(gl);

            Assert.NotNull(capturedRequest);
            Assert.Equal("false", capturedRequest!.Fields[Tokens.ArgsBulkResults]);
        }

        /// <summary>
        ///     Creates a mock IGremlinClient that captures the submitted RequestMessage.
        /// </summary>
        private static IGremlinClient CreateCapturingClient(Action<RequestMessage> capture)
        {
            var client = Substitute.For<IGremlinClient>();
            client.SubmitAsync<Traverser>(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
                .Returns(callInfo =>
                {
                    capture(callInfo.Arg<RequestMessage>());
                    var emptyResult = new ResultSet<Traverser>(
                        new List<Traverser>(), new Dictionary<string, object>());
                    return Task.FromResult(emptyResult);
                });
            return client;
        }
    }
}
