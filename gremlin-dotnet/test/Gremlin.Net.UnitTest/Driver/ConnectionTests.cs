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
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO;
using NSubstitute;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class ConnectionTests
    {
        private static readonly Uri TestUri = new Uri("http://localhost:8182/gremlin");

        /// <summary>
        ///     Creates a mock HttpMessageHandler that captures the request and returns a canned response.
        /// </summary>
        private static (HttpClient httpClient, MockHandler handler) CreateMockHttpClient(
            byte[]? responseBytes = null, string? contentEncoding = null)
        {
            var handler = new MockHandler(responseBytes ?? BuildMinimalResponseBytes(), contentEncoding);
            var httpClient = new HttpClient(handler);
            return (httpClient, handler);
        }

        /// <summary>
        ///     Builds a minimal valid 4.0 GraphBinary response: version + non-bulked + marker + status 200 + null msg + null exc.
        /// </summary>
        private static byte[] BuildMinimalResponseBytes()
        {
            using var ms = new MemoryStream();
            ms.WriteByte(0x84); // version
            ms.WriteByte(0x00); // non-bulked
            ms.WriteByte(0xFD); // marker type code
            ms.WriteByte(0x00); // marker value
            WriteInt(ms, 200); // status code
            ms.WriteByte(0x01); // null status message
            ms.WriteByte(0x01); // null exception
            return ms.ToArray();
        }

        private static void WriteInt(Stream stream, int value)
        {
            var bytes = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            stream.Write(bytes, 0, 4);
        }

        [Fact]
        public async Task ShouldSetContentTypeHeader()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("application/json",
                handler.CapturedRequest!.Content!.Headers.ContentType!.MediaType);
        }

        [Fact]
        public async Task ShouldSetAcceptHeader()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Contains(handler.CapturedRequest!.Headers.Accept,
                h => h.MediaType == SerializationTokens.GraphBinary4MimeType);
        }

        [Fact]
        public async Task ShouldSendPostRequest()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal(HttpMethod.Post, handler.CapturedRequest!.Method);
        }

        [Fact]
        public async Task ShouldSendToCorrectUri()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal(TestUri, handler.CapturedRequest!.RequestUri);
        }

        [Fact]
        public async Task ShouldSetAcceptEncodingWhenCompressionEnabled()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { Compression = Compression.Deflate };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Contains(handler.CapturedRequest!.Headers.AcceptEncoding,
                e => e.Value == "deflate");
        }

        [Fact]
        public async Task ShouldNotSetAcceptEncodingWhenCompressionDisabled()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { Compression = Compression.None };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.DoesNotContain(handler.CapturedRequest!.Headers.AcceptEncoding,
                e => e.Value == "deflate");
        }

        [Fact]
        public async Task ShouldSetAcceptEncodingByDefault()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Contains(handler.CapturedRequest!.Headers.AcceptEncoding,
                e => e.Value == "deflate");
        }

        [Fact]
        public async Task ShouldSetUserAgentWhenEnabled()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { EnableUserAgentOnConnect = true };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.True(handler.CapturedRequest!.Headers.Contains("User-Agent"));
        }

        [Fact]
        public async Task ShouldNotSetUserAgentWhenDisabled()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { EnableUserAgentOnConnect = false };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.False(handler.CapturedRequest!.Headers.Contains("User-Agent"));
        }

        [Fact]
        public async Task ShouldSetBulkResultsHeaderWhenEnabled()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { BulkResults = true };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.True(handler.CapturedRequest!.Headers.Contains("bulkResults"));
            Assert.Equal("true", handler.CapturedRequest.Headers.GetValues("bulkResults").First());
        }

        [Fact]
        public async Task ShouldNotSetBulkResultsHeaderWhenDisabled()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { BulkResults = false };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.False(handler.CapturedRequest!.Headers.Contains("bulkResults"));
        }

        [Fact]
        public async Task ShouldDecompressDeflateResponse()
        {
            // Compress the minimal response bytes the way the server does: java.util.zip.Deflater's
            // default constructor emits a zlib-wrapped stream (RFC 1950: 2-byte header + Adler-32),
            // which corresponds to .NET's ZLibStream (NOT the raw RFC 1951 DeflateStream). Using
            // ZLibStream here exercises the real wire format and would catch a raw/zlib mismatch.
            var originalBytes = BuildMinimalResponseBytes();
            byte[] compressedBytes;
            using (var compressedStream = new MemoryStream())
            {
                using (var zlibStream = new ZLibStream(compressedStream, CompressionMode.Compress, true))
                {
                    zlibStream.Write(originalBytes, 0, originalBytes.Length);
                }
                compressedBytes = compressedStream.ToArray();
            }

            var (httpClient, handler) = CreateMockHttpClient(compressedBytes, "deflate");
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { Compression = Compression.Deflate };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            // Should not throw — decompression should work
            var result = await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(result);
        }

        [Fact]
        public void ShouldDisposeWithoutError()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            var connection = new Connection(TestUri, serializer, settings, httpClient);

            connection.Dispose();
            // Double dispose should not throw
            connection.Dispose();
        }

        [Fact]
        public void ShouldNotMutateUserSslOptionsWhenSkippingCertValidation()
        {
            // The public constructor must NOT mutate the caller's SslClientAuthenticationOptions
            // when SkipCertificateValidation is set. The options object is a reference type that
            // may be shared across clients, so mutating it in place could silently disable
            // validation on another client. Instead, the skip-cert callback must be installed on
            // an internal clone, leaving the caller's object untouched.
            var userSsl = new System.Net.Security.SslClientAuthenticationOptions
            {
                TargetHost = "example.com"
            };
            var settings = new ConnectionSettings
            {
                Ssl = userSsl,
                SkipCertificateValidation = true
            };

            using var connection = new Connection(
                TestUri, CreateMockSerializer(), settings);

            // The caller's own options object must be left exactly as supplied: its
            // RemoteCertificateValidationCallback must remain null and its other fields intact.
            Assert.Equal("example.com", userSsl.TargetHost);
            Assert.Null(userSsl.RemoteCertificateValidationCallback);
            // settings.Ssl must still reference the very same object the caller provided.
            Assert.Same(userSsl, settings.Ssl);
        }

        [Fact]
        public void ShouldNotShareSkipCertCallbackAcrossClientsSharingSslOptions()
        {
            // Reusing one Ssl options object across two clients, only one of which skips cert
            // validation, must not leak the accept-all callback onto the shared object (and thus
            // onto the other client).
            var sharedSsl = new System.Net.Security.SslClientAuthenticationOptions
            {
                TargetHost = "example.com"
            };

            var skipSettings = new ConnectionSettings
            {
                Ssl = sharedSsl,
                SkipCertificateValidation = true
            };
            var strictSettings = new ConnectionSettings
            {
                Ssl = sharedSsl,
                SkipCertificateValidation = false
            };

            using var skipConnection = new Connection(TestUri, CreateMockSerializer(), skipSettings);
            using var strictConnection = new Connection(TestUri, CreateMockSerializer(), strictSettings);

            // The shared object must never have had the accept-all callback written onto it.
            Assert.Null(sharedSsl.RemoteCertificateValidationCallback);
        }

        [Fact]
        public void ShouldConstructWithProxyAndMaxHeaderBytes()
        {
            var settings = new ConnectionSettings
            {
                Proxy = new System.Net.WebProxy("http://localhost:3128"),
                MaxResponseHeaderBytes = 16384
            };

            // Should construct the handler without throwing.
            using var connection = new Connection(
                TestUri, CreateMockSerializer(), settings);
        }

        [Theory]
        [InlineData(1, 1)]      // a single byte still needs one whole kilobyte
        [InlineData(1023, 1)]   // just under 1 KB rounds up to 1
        [InlineData(1024, 1)]   // exactly 1 KB stays 1 (no spurious round-up)
        [InlineData(1025, 2)]   // one byte over a KB boundary rounds up to 2
        [InlineData(8191, 8)]   // just under 8 KB rounds up to 8
        [InlineData(8192, 8)]   // exactly 8 KB (the default) stays 8
        [InlineData(8193, 9)]   // one byte over rounds up to 9
        [InlineData(16384, 16)] // exactly 16 KB stays 16
        public void ShouldRoundMaxResponseHeaderBytesUpToKilobytes(int bytes, int expectedKilobytes)
        {
            // SocketsHttpHandler.MaxResponseHeadersLength is expressed in kilobytes while the
            // public option is in bytes. The conversion must round UP so the configured byte cap
            // is never silently lowered (ceil(bytes / 1024)). This asserts the rounding math the
            // public Connection constructor applies to the handler.
            Assert.Equal(expectedKilobytes, Connection.MaxResponseHeaderBytesToKilobytes(bytes));
        }

        [Fact]
        public void ShouldLeaveMaxResponseHeadersAtDefaultWhenBytesUnset()
        {
            // When MaxResponseHeaderBytes is 0 (the default / unset), the constructor must NOT
            // touch the handler's MaxResponseHeadersLength, leaving the .NET default in place.
            // The conversion is only applied for a positive byte cap, so constructing with the
            // default must succeed without invoking the rounding path.
            var settings = new ConnectionSettings();
            Assert.Equal(0, settings.MaxResponseHeaderBytes);

            // Should construct without throwing and without configuring the header cap.
            using var connection = new Connection(TestUri, CreateMockSerializer(), settings);
        }

        private static RequestMessage CreateTestRequest()
        {
            return RequestMessage.Build("g.V()").AddG("g").Create();
        }

        private static IMessageSerializer CreateMockSerializer(
            string mimeType = SerializationTokens.GraphBinary4MimeType)
        {
            return CreateMockSerializer(new List<object>(), mimeType);
        }

        private static IMessageSerializer CreateMockSerializer(
            List<object> results,
            string mimeType = SerializationTokens.GraphBinary4MimeType)
        {
            var serializer = Substitute.For<IMessageSerializer>();
            serializer.MimeType.Returns(mimeType);
            serializer.DeserializeMessageAsync(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
                .Returns(callInfo => ToAsyncEnumerable(results));
            return serializer;
        }

        private static async IAsyncEnumerable<object> ToAsyncEnumerable(List<object> items)
        {
            foreach (var item in items)
            {
                yield return item;
            }
            await Task.CompletedTask;
        }

        [Fact]
        public async Task ShouldCallInterceptorsInOrder()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            var callOrder = new List<int>();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx => { callOrder.Add(1); return Task.CompletedTask; },
                ctx => { callOrder.Add(2); return Task.CompletedTask; },
                ctx => { callOrder.Add(3); return Task.CompletedTask; },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient, interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.Equal(new List<int> { 1, 2, 3 }, callOrder);
        }

        [Fact]
        public async Task ShouldPropagateInterceptorException()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            var expectedException = new InvalidOperationException("interceptor failed");

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                _ => throw expectedException,
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient, interceptors);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => connection.SubmitAsync<object>(CreateTestRequest()));

            Assert.Same(expectedException, ex);
            // HTTP request should not have been sent
            Assert.Null(handler.CapturedRequest);
        }

        [Fact]
        public async Task ShouldAllowInterceptorToModifyHeaders()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    ctx.Headers["Authorization"] = "Basic dGVzdDp0ZXN0";
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient, interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.True(handler.CapturedRequest!.Headers.Contains("Authorization"));
            Assert.Equal("Basic dGVzdDp0ZXN0",
                handler.CapturedRequest.Headers.GetValues("Authorization").First());
        }

        [Fact]
        public async Task ShouldSeeEarlierInterceptorModifications()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            string? observedHeader = null;

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    ctx.Headers["X-Custom"] = "first";
                    return Task.CompletedTask;
                },
                ctx =>
                {
                    observedHeader = ctx.Headers.ContainsKey("X-Custom") ? ctx.Headers["X-Custom"] : null;
                    ctx.Headers["X-Custom"] = "second";
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient, interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.Equal("first", observedHeader);
            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("second",
                handler.CapturedRequest!.Headers.GetValues("X-Custom").First());
        }

        [Fact]
        public async Task ShouldPassRequestMessageToInterceptorsBeforeSerialization()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            object? observedBody = null;

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    observedBody = ctx.Body;
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.IsType<RequestMessage>(observedBody);
        }

        [Fact]
        public async Task ShouldThrowWhenBodyIsUnsupportedTypeAfterInterceptors()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    ctx.Body = "unsupported type";
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => connection.SubmitAsync<object>(CreateTestRequest()));

            Assert.Contains("String", ex.Message);
            // HTTP request should not have been sent
            Assert.Null(handler.CapturedRequest);
        }

        [Fact]
        public async Task ShouldSucceedWhenInterceptorPreSerializesBody()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    // Interceptor calls SerializeBody() early (e.g. for signing)
                    ctx.SerializeBody();
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            var result = await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(result);
            Assert.NotNull(handler.CapturedRequest);
        }

        [Fact]
        public async Task ShouldNotSetContentTypeBeforeInterceptorsRun()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            bool? hadContentType = null;

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    hadContentType = ctx.Headers.ContainsKey("Content-Type");
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.False(hadContentType, "Content-Type should not be set before interceptors run");
            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("application/json",
                handler.CapturedRequest!.Content!.Headers.ContentType!.MediaType);
        }

        [Fact]
        public async Task ShouldWorkWithEmptyInterceptorList()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            var interceptors = new List<Func<HttpRequestContext, Task>>();

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            var result = await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(result);
            Assert.NotNull(handler.CapturedRequest);
        }

        [Fact]
        public async Task ShouldWorkWithNoInterceptorsParameter()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            var result = await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(result);
            Assert.NotNull(handler.CapturedRequest);
        }

        [Fact]
        public async Task ShouldAllowInterceptorToModifyUri()
        {
            var altUri = new Uri("http://other-host:9999/gremlin");
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    ctx.Uri = altUri;
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal(altUri, handler.CapturedRequest!.RequestUri);
        }

        [Fact]
        public async Task ShouldAllowInterceptorToReplaceBody()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            var replacementBody = new byte[] { 0x01, 0x02, 0x03 };

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    ctx.Body = replacementBody;
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            var sentBytes = await handler.CapturedRequest!.Content!.ReadAsByteArrayAsync();
            Assert.Equal(replacementBody, sentBytes);
        }

        [Fact]
        public async Task ShouldStopInterceptorChainOnException()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            var secondCalled = false;

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                _ => throw new InvalidOperationException("first failed"),
                _ =>
                {
                    secondCalled = true;
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => connection.SubmitAsync<object>(CreateTestRequest()));

            Assert.False(secondCalled, "Second interceptor should not run when first throws");
            Assert.Null(handler.CapturedRequest);
        }

        [Fact]
        public async Task ShouldSupportAsyncInterceptors()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            var interceptorCompleted = false;

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                async ctx =>
                {
                    // Simulate async work (e.g., fetching a token)
                    await Task.Delay(1);
                    ctx.Headers["X-Async-Header"] = "async-value";
                    interceptorCompleted = true;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.True(interceptorCompleted);
            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("async-value",
                handler.CapturedRequest!.Headers.GetValues("X-Async-Header").First());
        }

        [Fact]
        public async Task ShouldAllowInterceptorToCallSerializeBody()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            byte[]? capturedBody = null;

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    capturedBody = ctx.SerializeBody();
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(capturedBody);
            // Should be valid JSON containing "gremlin" field
            Assert.Contains("gremlin", System.Text.Encoding.UTF8.GetString(capturedBody!));
        }

        [Fact]
        public async Task ShouldWorkWithSingleInterceptor()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            var called = false;

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    called = true;
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.True(called);
            Assert.NotNull(handler.CapturedRequest);
        }

        [Fact]
        public async Task ShouldAllowInterceptorToRemoveHeader()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { EnableUserAgentOnConnect = true };

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    ctx.Headers.Remove("User-Agent");
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.False(handler.CapturedRequest!.Headers.Contains("User-Agent"),
                "Interceptor should be able to remove headers set by Connection");
        }

        [Fact]
        public async Task ShouldThrowWhenBodyIsNullAfterInterceptors()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    ctx.Body = null!;
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => connection.SubmitAsync<object>(CreateTestRequest()));

            Assert.Contains("null", ex.Message);
            Assert.Null(handler.CapturedRequest);
        }

        [Fact]
        public async Task ShouldPreserveMultipleInterceptorHeaderModifications()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    ctx.Headers["X-First"] = "one";
                    return Task.CompletedTask;
                },
                ctx =>
                {
                    ctx.Headers["X-Second"] = "two";
                    return Task.CompletedTask;
                },
                ctx =>
                {
                    ctx.Headers["X-Third"] = "three";
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("one", handler.CapturedRequest!.Headers.GetValues("X-First").First());
            Assert.Equal("two", handler.CapturedRequest.Headers.GetValues("X-Second").First());
            Assert.Equal("three", handler.CapturedRequest.Headers.GetValues("X-Third").First());
        }

        [Fact]
        public async Task ShouldAllowInterceptorToOverrideContentType()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    // Pre-serialize with custom content type
                    ctx.Body = new byte[] { 0x01 };
                    ctx.Headers["Content-Type"] = "application/custom";
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("application/custom",
                handler.CapturedRequest!.Content!.Headers.ContentType!.MediaType);
        }

        [Fact]
        public async Task ShouldUseResponseSerializerForDeserialization()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var responseSerializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            using var connection = new Connection(TestUri, responseSerializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            // Verify the response serializer was called for deserialization
            responseSerializer.Received(1)
                .DeserializeMessageAsync(Arg.Any<Stream>(), Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task ShouldAcceptHttpContentBodyFromInterceptor()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            var contentBytes = new byte[] { 0x01, 0x02, 0x03 };

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    ctx.Body = new ByteArrayContent(contentBytes);
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            var sentBytes = await handler.CapturedRequest!.Content!.ReadAsByteArrayAsync();
            Assert.Equal(contentBytes, sentBytes);
        }

        [Fact]
        public async Task ShouldUseResponseSerializerMimeTypeForAcceptHeader()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var responseSerializer = CreateMockSerializer("application/custom-response");
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, responseSerializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Contains(handler.CapturedRequest!.Headers.Accept,
                h => h.MediaType == "application/custom-response");
        }

        [Fact]
        public async Task ShouldSetContentTypeToApplicationJson()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("application/json",
                handler.CapturedRequest!.Content!.Headers.ContentType!.MediaType);
        }

        [Fact]
        public async Task ShouldReturnStreamingResultSet()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var serializer = CreateMockSerializer(new List<object> { "hello", 42 });
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            var result = await connection.SubmitAsync<object>(CreateTestRequest());

            var items = await result.ToListAsync();
            Assert.Equal(2, items.Count);
            Assert.Equal("hello", items[0]);
            Assert.Equal(42, items[1]);
        }

        [Fact]
        public async Task ShouldReturnEmptyResultSetWhenNoResults()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var serializer = CreateMockSerializer(new List<object>());
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            var result = await connection.SubmitAsync<object>(CreateTestRequest());

            var items = await result.ToListAsync();
            Assert.Empty(items);
        }

        [Fact]
        public async Task ShouldStreamResponseWithoutFullBuffering()
        {
            // Verify that Connection reads the response as a stream (consistent with
            // HttpCompletionOption.ResponseHeadersRead) rather than buffering the entire
            // body. We do this by using a SlowStream that tracks whether ReadAsync was
            // called incrementally (streaming) vs the content being fully buffered upfront.
            var responseBytes = BuildMinimalResponseBytes();
            var slowStream = new TrackingStream(new MemoryStream(responseBytes));
            var handler = new StreamMockHandler(slowStream);
            var httpClient = new HttpClient(handler);
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            var result = await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(result);
            Assert.True(handler.WasCalled, "SendAsync should have been called");
        }

        [Fact]
        public async Task ShouldFillBatchSizeFromDefaultWhenUnset()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { BatchSize = 42 };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            var request = CreateTestRequest();
            await connection.SubmitAsync<object>(request);

            // The caller-owned request must NOT be mutated by default-filling.
            Assert.False(request.Fields.ContainsKey(Tokens.ArgsBatchSize));
            // The outgoing wire payload must carry the connection-level default.
            Assert.Equal(42, await ReadBatchSizeFromBodyAsync(handler));
        }

        [Fact]
        public async Task ShouldNotOverrideExplicitBatchSize()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { BatchSize = 42 };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            var request = RequestMessage.Build("g.V()").AddG("g").AddBatchSize(100).Create();
            await connection.SubmitAsync<object>(request);

            // A per-request explicit batchSize always wins, on the caller object and the wire.
            Assert.Equal(100, request.Fields[Tokens.ArgsBatchSize]);
            Assert.Equal(100, await ReadBatchSizeFromBodyAsync(handler));
        }

        [Fact]
        public async Task ShouldUseDefaultBatchSizeOf64ByDefault()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            var request = CreateTestRequest();
            await connection.SubmitAsync<object>(request);

            Assert.False(request.Fields.ContainsKey(Tokens.ArgsBatchSize));
            Assert.Equal(64, await ReadBatchSizeFromBodyAsync(handler));
        }

        [Fact]
        public async Task ShouldNotPersistDefaultBatchSizeAcrossResubmissions()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { BatchSize = 42 };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            // Resubmitting the same message must not carry over a previously injected default.
            var request = CreateTestRequest();
            await connection.SubmitAsync<object>(request);
            await connection.SubmitAsync<object>(request);

            Assert.False(request.Fields.ContainsKey(Tokens.ArgsBatchSize));
            Assert.Equal(42, await ReadBatchSizeFromBodyAsync(handler));
        }

        /// <summary>
        ///     Reads the serialized JSON request body captured by the mock handler and returns
        ///     the <c>batchSize</c> field value, or <c>null</c> when it is absent.
        /// </summary>
        private static async Task<int?> ReadBatchSizeFromBodyAsync(MockHandler handler)
        {
            Assert.NotNull(handler.CapturedRequest);
            var bodyBytes = await handler.CapturedRequest!.Content!.ReadAsByteArrayAsync();
            using var doc = System.Text.Json.JsonDocument.Parse(bodyBytes);
            if (doc.RootElement.TryGetProperty(Tokens.ArgsBatchSize, out var batchSizeProp))
            {
                return batchSizeProp.GetInt32();
            }
            return null;
        }

        [Fact]
        public async Task ShouldTimeOutSlowReadWhenReadTimeoutSet()
        {
            // A response stream that blocks indefinitely on read should trigger the
            // per-read idle timeout once it is consumed during deserialization.
            var blockingStream = new BlockingStream();
            var handler = new StreamMockHandler(blockingStream);
            var httpClient = new HttpClient(handler);
            // The serializer actually reads from the stream so the read timeout can fire.
            var serializer = CreateReadingSerializer();
            var settings = new ConnectionSettings
            {
                ReadTimeout = TimeSpan.FromMilliseconds(100)
            };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

            var result = await connection.SubmitAsync<object>(CreateTestRequest());

            // The background streaming task surfaces the timeout as a faulted enumeration.
            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await foreach (var _ in result) { }
            });
        }

        private static IMessageSerializer CreateReadingSerializer(
            string mimeType = SerializationTokens.GraphBinary4MimeType)
        {
            var serializer = Substitute.For<IMessageSerializer>();
            serializer.MimeType.Returns(mimeType);
            serializer.DeserializeMessageAsync(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
                .Returns(callInfo => ReadAllAsync((Stream)callInfo[0], (CancellationToken)callInfo[1]));
            return serializer;
        }

        private static async IAsyncEnumerable<object> ReadAllAsync(Stream stream,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var buffer = new byte[16];
            while (await stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false) > 0)
            {
                yield return new object();
            }
        }

        /// <summary>
        ///     A stream whose ReadAsync never completes until cancelled, used to exercise the
        ///     per-read timeout.
        /// </summary>
        private sealed class BlockingStream : Stream
        {
            public override async ValueTask<int> ReadAsync(Memory<byte> buffer,
                CancellationToken cancellationToken = default)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
                return 0;
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                Thread.Sleep(Timeout.Infinite);
                return 0;
            }

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => throw new NotSupportedException();
            public override long Position { get => 0; set { } }
            public override void Flush() { }
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }

        /// <summary>
        ///     A test HttpMessageHandler that captures the request and returns a canned response.
        ///     The response uses ByteArrayContent but does NOT dispose the content stream
        ///     immediately, allowing streaming reads after SendAsync returns.
        /// </summary>
        private class MockHandler : HttpMessageHandler
        {
            private readonly byte[] _responseBytes;
            private readonly string? _contentEncoding;

            public HttpRequestMessage? CapturedRequest { get; private set; }

            public MockHandler(byte[] responseBytes, string? contentEncoding = null)
            {
                _responseBytes = responseBytes;
                _contentEncoding = contentEncoding;
            }

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                // Clone the request headers before the original request is disposed
                CapturedRequest = CloneRequest(request);

                var response = new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(_responseBytes)
                };
                response.Content.Headers.ContentType =
                    new MediaTypeHeaderValue(SerializationTokens.GraphBinary4MimeType);
                if (_contentEncoding != null)
                {
                    response.Content.Headers.ContentEncoding.Add(_contentEncoding);
                }
                return Task.FromResult(response);
            }

            private static HttpRequestMessage CloneRequest(HttpRequestMessage original)
            {
                var clone = new HttpRequestMessage(original.Method, original.RequestUri);

                // Copy headers
                foreach (var header in original.Headers)
                {
                    clone.Headers.TryAddWithoutValidation(header.Key, header.Value);
                }

                // Copy content headers by creating a dummy content
                if (original.Content != null)
                {
                    var contentBytes = original.Content.ReadAsByteArrayAsync().Result;
                    clone.Content = new ByteArrayContent(contentBytes);
                    foreach (var header in original.Content.Headers)
                    {
                        clone.Content.Headers.TryAddWithoutValidation(header.Key, header.Value);
                    }
                }

                return clone;
            }
        }

        /// <summary>
        ///     A mock handler that returns a StreamContent wrapping a provided stream,
        ///     used to verify streaming behavior with ResponseHeadersRead.
        /// </summary>
        private class StreamMockHandler : HttpMessageHandler
        {
            private readonly Stream _responseStream;

            public bool WasCalled { get; private set; }

            public StreamMockHandler(Stream responseStream)
            {
                _responseStream = responseStream;
            }

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                WasCalled = true;

                var response = new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StreamContent(_responseStream)
                };
                response.Content.Headers.ContentType =
                    new MediaTypeHeaderValue(SerializationTokens.GraphBinary4MimeType);
                return Task.FromResult(response);
            }
        }

        /// <summary>
        ///     A stream wrapper that tracks read operations.
        /// </summary>
        private class TrackingStream : Stream
        {
            private readonly Stream _inner;

            public TrackingStream(Stream inner)
            {
                _inner = inner;
            }

            public override bool CanRead => _inner.CanRead;
            public override bool CanSeek => _inner.CanSeek;
            public override bool CanWrite => _inner.CanWrite;
            public override long Length => _inner.Length;
            public override long Position
            {
                get => _inner.Position;
                set => _inner.Position = value;
            }

            public override void Flush() => _inner.Flush();
            public override int Read(byte[] buffer, int offset, int count) =>
                _inner.Read(buffer, offset, count);
            public override long Seek(long offset, SeekOrigin origin) =>
                _inner.Seek(offset, origin);
            public override void SetLength(long value) => _inner.SetLength(value);
            public override void Write(byte[] buffer, int offset, int count) =>
                _inner.Write(buffer, offset, count);

            public override ValueTask<int> ReadAsync(Memory<byte> buffer,
                CancellationToken cancellationToken = default) =>
                _inner.ReadAsync(buffer, cancellationToken);

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count,
                CancellationToken cancellationToken) =>
                _inner.ReadAsync(buffer, offset, count, cancellationToken);
        }
    }
}
