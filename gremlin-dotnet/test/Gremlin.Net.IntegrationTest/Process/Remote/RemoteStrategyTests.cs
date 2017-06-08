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
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Remote
{
    public class RemoteStrategyTests
    {
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"];
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);

        [Fact]
        public void ShouldSendBytecodeToGremlinServer()
        {
            const string expectedResult = "gremlin";
            var testBytecode = new Bytecode();
            testBytecode.AddStep("V");
            testBytecode.AddStep("has", "test");
            testBytecode.AddStep("inject", expectedResult);
            var testTraversal = CreateTraversalWithRemoteStrategy(testBytecode);

            var actualResult = testTraversal.Next();

            Assert.Equal(expectedResult, actualResult);
        }

        [Fact]
        public async Task ShouldSendBytecodeToGremlinServerAsynchronouslyForTraversalPromise()
        {
            const string expectedResult = "gremlin";
            var testBytecode = new Bytecode();
            testBytecode.AddStep("V");
            testBytecode.AddStep("has", "test");
            testBytecode.AddStep("inject", expectedResult);
            var testTraversal = CreateTraversalWithRemoteStrategy(testBytecode);

            var actualResult = await testTraversal.Promise(t => t.Next());

            Assert.Equal(expectedResult, actualResult);
        }

        private DefaultTraversal CreateTraversalWithRemoteStrategy(Bytecode bytecode)
        {
            var remoteStrategy =
                new RemoteStrategy(new DriverRemoteConnection(new GremlinClient(new GremlinServer(TestHost, TestPort))));
            return new TestTraversal(remoteStrategy, bytecode);
        }
    }

    internal class TestTraversal : DefaultTraversal
    {
        public TestTraversal(ITraversalStrategy traversalStrategy, Bytecode bytecode)
        {
            TraversalStrategies.Add(traversalStrategy);
            Bytecode = bytecode;
        }
    }
}