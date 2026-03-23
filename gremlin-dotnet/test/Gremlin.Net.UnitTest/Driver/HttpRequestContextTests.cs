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
using System.Text;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class HttpRequestContextTests
    {
        [Fact]
        public void ShouldConstructWithByteArrayBody()
        {
            var method = "POST";
            var uri = new Uri("http://localhost:8182/gremlin");
            var headers = new Dictionary<string, string> { { "Content-Type", "application/vnd.graphbinary-v4.0" } };
            var body = new byte[] { 0x01, 0x02, 0x03 };

            var context = new HttpRequestContext(method, uri, headers, body);

            Assert.Equal(method, context.Method);
            Assert.Equal(uri, context.Uri);
            Assert.Same(headers, context.Headers);
            Assert.Same(body, context.Body);
        }

        [Fact]
        public void ShouldConstructWithRequestMessageBody()
        {
            var method = "POST";
            var uri = new Uri("http://localhost:8182/gremlin");
            var headers = new Dictionary<string, string>();
            var body = RequestMessage.Build("g.V()").AddG("g").Create();

            var context = new HttpRequestContext(method, uri, headers, body);

            Assert.Same(body, context.Body);
            Assert.IsType<RequestMessage>(context.Body);
        }

        [Fact]
        public void ShouldAllowMutatingProperties()
        {
            var context = new HttpRequestContext("POST", new Uri("http://localhost:8182/gremlin"),
                new Dictionary<string, string>(), new byte[] { 0x01 });

            var newUri = new Uri("https://example.com/gremlin");
            context.Method = "PUT";
            context.Uri = newUri;
            context.Body = new byte[] { 0x02, 0x03 };
            context.Headers["Authorization"] = "Basic dGVzdA==";

            Assert.Equal("PUT", context.Method);
            Assert.Equal(newUri, context.Uri);
            Assert.Equal(new byte[] { 0x02, 0x03 }, context.Body);
            Assert.Equal("Basic dGVzdA==", context.Headers["Authorization"]);
        }

        [Fact]
        public void ShouldComputePayloadHashForKnownBody()
        {
            // SHA-256 of "hello" = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
            var body = Encoding.UTF8.GetBytes("hello");
            var context = new HttpRequestContext("POST", new Uri("http://localhost:8182/gremlin"),
                new Dictionary<string, string>(), body);

            var hash = context.GetPayloadHash();

            Assert.Equal("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", hash);
        }

        [Fact]
        public void ShouldComputePayloadHashForEmptyBody()
        {
            var context = new HttpRequestContext("POST", new Uri("http://localhost:8182/gremlin"),
                new Dictionary<string, string>(), Array.Empty<byte>());

            var hash = context.GetPayloadHash();

            Assert.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", hash);
        }

        [Fact]
        public void ShouldThrowWhenComputingPayloadHashForNonByteArrayBody()
        {
            var body = RequestMessage.Build("g.V()").AddG("g").Create();
            var context = new HttpRequestContext("POST", new Uri("http://localhost:8182/gremlin"),
                new Dictionary<string, string>(), body);

            var ex = Assert.Throws<InvalidOperationException>(() => context.GetPayloadHash());

            Assert.Contains("RequestMessage", ex.Message);
            Assert.Contains("byte[]", ex.Message);
        }

        [Fact]
        public void ShouldThrowWhenComputingPayloadHashForNullBody()
        {
            var context = new HttpRequestContext("POST", new Uri("http://localhost:8182/gremlin"),
                new Dictionary<string, string>(), null!);

            var ex = Assert.Throws<InvalidOperationException>(() => context.GetPayloadHash());

            Assert.Contains("null", ex.Message);
        }
    }
}
