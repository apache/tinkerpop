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
            var msg = RequestMessage.Build("g.V()").AddG("g").Create();

            Assert.Equal("g", msg.Fields[Tokens.ArgsG]);
        }

        [Fact]
        public void ShouldSetBindings()
        {
            var msg = RequestMessage.Build("g.V(_0)")
                .AddBinding("_0", 1)
                .Create();

            var bindings = (Dictionary<string, object>)msg.Fields[Tokens.ArgsBindings];
            Assert.Equal(1, bindings["_0"]);
        }

        [Fact]
        public void ShouldSetMultipleBindings()
        {
            var bindings = new Dictionary<string, object> { { "_0", 1 }, { "_1", "test" } };
            var msg = RequestMessage.Build("g.V(_0).has(_1)")
                .AddBindings(bindings)
                .Create();

            var resultBindings = (Dictionary<string, object>)msg.Fields[Tokens.ArgsBindings];
            Assert.Equal(1, resultBindings["_0"]);
            Assert.Equal("test", resultBindings["_1"]);
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
        public void ShouldIncludeEmptyBindingsWhenNoneAdded()
        {
            var msg = RequestMessage.Build("g.V()").Create();

            var bindings = (Dictionary<string, object>)msg.Fields[Tokens.ArgsBindings];
            Assert.Empty(bindings);
        }
    }
}
