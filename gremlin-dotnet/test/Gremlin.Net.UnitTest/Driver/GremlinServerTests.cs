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

using Gremlin.Net.Driver;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class GremlinServerTests
    {
        [Theory]
        [InlineData("localhost", 8182)]
        [InlineData("1.2.3.4", 5678)]
        public void ShouldBuildCorrectUri(string host, int port)
        {
            var gremlinServer = new GremlinServer(host, port);

            var uri = gremlinServer.Uri;

            Assert.Equal($"ws://{host}:{port}/gremlin", uri.AbsoluteUri);
        }

        [Fact]
        public void ShouldBuildCorrectUriForSsl()
        {
            var host = "localhost";
            var port = 8181;
            var gremlinServer = new GremlinServer(host, port, true);

            var uri = gremlinServer.Uri;

            Assert.Equal($"wss://{host}:{port}/gremlin", uri.AbsoluteUri);
        }

        [Fact]
        public void ShouldUseCorrectDefaultPortWhenNoneProvided()
        {
            var host = "testHost";
            var gremlinServer = new GremlinServer(host);

            var uri = gremlinServer.Uri;

            Assert.Equal(8182, uri.Port);
        }
    }
}