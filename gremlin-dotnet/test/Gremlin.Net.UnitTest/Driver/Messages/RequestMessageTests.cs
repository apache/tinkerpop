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

using System.Collections.Generic;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver.Messages
{
    public class RequestMessageTests
    {
        [Fact]
        public void ShouldSetGremlinProperty()
        {
            var msg = RequestMessage.Build("g.V()").Create();

            Assert.Equal("g.V()", msg.Gremlin);
        }

        [Fact]
        public void ShouldSetDefaultLanguageField()
        {
            var msg = RequestMessage.Build("g.V()").Create();

            Assert.Equal("gremlin-lang", msg.Fields[Tokens.ArgsLanguage]);
        }

        [Fact]
        public void ShouldSetGField()
        {
            var msg = RequestMessage.Build("g.V()").AddG("customG").Create();

            Assert.Equal("customG", msg.Fields[Tokens.ArgsG]);
        }

        [Fact]
        public void ShouldSetParameters()
        {
            var msg = RequestMessage.Build("g.V(x)")
                .AddParameter("x", 1)
                .Create();

            var parametersString = (string)msg.Fields[Tokens.ArgsParameters];
            Assert.Contains("\"x\":1", parametersString);
        }

        [Fact]
        public void ShouldSetMultipleParameters()
        {
            var parameters = new Dictionary<string, object> { { "x", 1 }, { "name", "test" } };
            var msg = RequestMessage.Build("g.V(x).has(name)")
                .AddParameters(parameters)
                .Create();

            var parametersString = (string)msg.Fields[Tokens.ArgsParameters];
            Assert.Contains("\"x\":1", parametersString);
            Assert.Contains("\"name\":\"test\"", parametersString);
        }

        [Fact]
        public void ShouldSetAdditionalField()
        {
            var msg = RequestMessage.Build("g.V()")
                .AddField(Tokens.ArgsEvalTimeout, 5000L)
                .Create();

            Assert.Equal(5000L, msg.Fields[Tokens.ArgsEvalTimeout]);
        }

        [Fact]
        public void ShouldReportHasField()
        {
            var builder = RequestMessage.Build("g.V()")
                .AddField(Tokens.ArgsBulkResults, "true");

            Assert.True(builder.HasField(Tokens.ArgsBulkResults));
            Assert.False(builder.HasField("nonexistent"));
        }

        [Fact]
        public void ShouldNotContainParametersWhenNoneAdded()
        {
            var msg = RequestMessage.Build("g.V()").Create();

            Assert.False(msg.Fields.ContainsKey(Tokens.ArgsParameters));
        }

        [Fact]
        public void ShouldSetParametersString()
        {
            var msg = RequestMessage.Build("g.V(x)")
                .AddParametersString("[\"x\":1]")
                .Create();

            Assert.Equal("[\"x\":1]", msg.Fields[Tokens.ArgsParameters]);
        }
    }
}
