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
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Amazon.Runtime;
using Gremlin.Net.Driver;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class AuthTests
    {
        private static HttpRequestContext CreateTestContext()
        {
            return new HttpRequestContext("POST", new Uri("http://localhost:8182/gremlin"),
                new Dictionary<string, string>(), new byte[] { 0x01 });
        }

        [Fact]
        public async Task BasicAuthShouldSetCorrectAuthorizationHeader()
        {
            var interceptor = Auth.BasicAuth("user", "pass");
            var context = CreateTestContext();

            await interceptor(context);

            var expected = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("user:pass"));
            Assert.Equal(expected, context.Headers["Authorization"]);
        }

        [Fact]
        public async Task BasicAuthShouldSetHeaderOnEveryInvocation()
        {
            var interceptor = Auth.BasicAuth("user", "pass");
            var context1 = CreateTestContext();
            var context2 = CreateTestContext();

            await interceptor(context1);
            await interceptor(context2);

            var expected = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("user:pass"));
            Assert.Equal(expected, context1.Headers["Authorization"]);
            Assert.Equal(expected, context2.Headers["Authorization"]);
        }

        [Fact]
        public async Task BasicAuthShouldHandleColonsInPassword()
        {
            var interceptor = Auth.BasicAuth("user", "pass:with:colons");
            var context = CreateTestContext();

            await interceptor(context);

            var expected = "Basic " + Convert.ToBase64String(
                Encoding.UTF8.GetBytes("user:pass:with:colons"));
            Assert.Equal(expected, context.Headers["Authorization"]);
        }

        [Fact]
        public async Task BasicAuthShouldHandleUnicodeCharacters()
        {
            var interceptor = Auth.BasicAuth("用户", "密码");
            var context = CreateTestContext();

            await interceptor(context);

            var expected = "Basic " + Convert.ToBase64String(
                Encoding.UTF8.GetBytes("用户:密码"));
            Assert.Equal(expected, context.Headers["Authorization"]);
        }

        [Fact]
        public async Task BasicAuthShouldOverwriteExistingAuthorizationHeader()
        {
            var interceptor = Auth.BasicAuth("user", "pass");
            var context = CreateTestContext();
            context.Headers["Authorization"] = "Bearer old-token";

            await interceptor(context);

            var expected = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("user:pass"));
            Assert.Equal(expected, context.Headers["Authorization"]);
        }

        [Fact]
        public async Task BasicAuthShouldHandleEmptyCredentials()
        {
            var interceptor = Auth.BasicAuth("", "");
            var context = CreateTestContext();

            await interceptor(context);

            var expected = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(":"));
            Assert.Equal(expected, context.Headers["Authorization"]);
        }

        // --- SigV4 Tests ---

        private static readonly BasicAWSCredentials TestBasicCredentials =
            new BasicAWSCredentials("MOCK_ID", "MOCK_KEY");

        private static readonly SessionAWSCredentials TestSessionCredentials =
            new SessionAWSCredentials("MOCK_ID", "MOCK_KEY", "MOCK_TOKEN");

        private static HttpRequestContext CreateSigv4TestContext(byte[]? body = null)
        {
            return new HttpRequestContext("POST", new Uri("https://example.com:8182/gremlin"),
                new Dictionary<string, string>
                {
                    { "Content-Type", "application/vnd.graphbinary-v4.0" },
                    { "Accept", "application/vnd.graphbinary-v4.0" },
                },
                body ?? new byte[] { 0x84, 0x00 });
        }

        [Fact]
        public async Task SigV4AuthShouldAddRequiredHeaders()
        {
            var interceptor = Auth.SigV4Auth("gremlin-east-1", "tinkerpop-sigv4", TestBasicCredentials);
            var context = CreateSigv4TestContext();

            await interceptor(context);

            Assert.True(context.Headers.ContainsKey("Authorization"));
            Assert.True(context.Headers.ContainsKey("X-Amz-Date"));
            Assert.True(context.Headers.ContainsKey("x-amz-content-sha256"));
            Assert.True(context.Headers.ContainsKey("Host"));
        }

        [Fact]
        public async Task SigV4AuthShouldHaveCorrectAuthorizationPrefix()
        {
            var interceptor = Auth.SigV4Auth("gremlin-west-2", "tinkerpop-sigv4", TestBasicCredentials);
            var context = CreateSigv4TestContext();

            await interceptor(context);

            Assert.StartsWith("AWS4-HMAC-SHA256 Credential=MOCK_ID", context.Headers["Authorization"]);
            Assert.Contains("gremlin-west-2/tinkerpop-sigv4/aws4_request", context.Headers["Authorization"]);
        }

        [Fact]
        public async Task SigV4AuthShouldAddSessionTokenForTemporaryCredentials()
        {
            var interceptor = Auth.SigV4Auth("gremlin-east-1", "tinkerpop-sigv4", TestSessionCredentials);
            var context = CreateSigv4TestContext();

            await interceptor(context);

            Assert.True(context.Headers.ContainsKey("X-Amz-Security-Token"));
            Assert.Equal("MOCK_TOKEN", context.Headers["X-Amz-Security-Token"]);
        }

        [Fact]
        public async Task SigV4AuthShouldNotAddSessionTokenForPermanentCredentials()
        {
            var interceptor = Auth.SigV4Auth("gremlin-east-1", "tinkerpop-sigv4", TestBasicCredentials);
            var context = CreateSigv4TestContext();

            await interceptor(context);

            Assert.False(context.Headers.ContainsKey("X-Amz-Security-Token"));
        }

        [Fact]
        public async Task SigV4AuthContentHashShouldMatchBodySha256()
        {
            var body = new byte[] { 0x84, 0x00, 0xFD, 0x01 };
            var interceptor = Auth.SigV4Auth("gremlin-east-1", "tinkerpop-sigv4", TestBasicCredentials);
            var context = CreateSigv4TestContext(body);

            await interceptor(context);

            using var sha256 = SHA256.Create();
            var expectedHash = BitConverter.ToString(sha256.ComputeHash(body))
                .Replace("-", "").ToLowerInvariant();
            Assert.Equal(expectedHash, context.Headers["x-amz-content-sha256"]);
        }

        [Fact]
        public async Task SigV4AuthShouldHandleEmptyBody()
        {
            var interceptor = Auth.SigV4Auth("gremlin-east-1", "tinkerpop-sigv4", TestBasicCredentials);
            var context = CreateSigv4TestContext(Array.Empty<byte>());

            await interceptor(context);

            Assert.True(context.Headers.ContainsKey("Authorization"));
            Assert.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                context.Headers["x-amz-content-sha256"]);
        }

        [Fact]
        public async Task SigV4AuthShouldSetCorrectHost()
        {
            var interceptor = Auth.SigV4Auth("gremlin-east-1", "tinkerpop-sigv4", TestBasicCredentials);
            var context = CreateSigv4TestContext();

            await interceptor(context);

            Assert.Equal("example.com", context.Headers["Host"]);
        }

        [Fact]
        public async Task SigV4AuthShouldThrowWhenBodyIsNotByteArray()
        {
            var interceptor = Auth.SigV4Auth("gremlin-east-1", "tinkerpop-sigv4", TestBasicCredentials);
            var context = new HttpRequestContext("POST", new Uri("https://example.com:8182/gremlin"),
                new Dictionary<string, string>
                {
                    { "Content-Type", "application/vnd.graphbinary-v4.0" },
                },
                "not-bytes");

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => interceptor(context));

            Assert.Contains("byte[]", ex.Message);
        }
    }
}
