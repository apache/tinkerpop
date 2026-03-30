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
using Gremlin.Net.Process.Traversal;
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
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal(SerializationTokens.GraphBinary4MimeType,
                handler.CapturedRequest!.Content!.Headers.ContentType!.MediaType);
        }

        [Fact]
        public async Task ShouldSetAcceptHeader()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

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
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

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
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal(TestUri, handler.CapturedRequest!.RequestUri);
        }

        [Fact]
        public async Task ShouldSetAcceptEncodingWhenCompressionEnabled()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { EnableCompression = true };
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

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
            var settings = new ConnectionSettings { EnableCompression = false };
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.DoesNotContain(handler.CapturedRequest!.Headers.AcceptEncoding,
                e => e.Value == "deflate");
        }

        [Fact]
        public async Task ShouldSetUserAgentWhenEnabled()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { EnableUserAgentOnConnect = true };
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

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
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

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
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

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
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.False(handler.CapturedRequest!.Headers.Contains("bulkResults"));
        }

        [Fact]
        public async Task ShouldDecompressDeflateResponse()
        {
            // Compress the minimal response bytes with deflate
            var originalBytes = BuildMinimalResponseBytes();
            byte[] compressedBytes;
            using (var compressedStream = new MemoryStream())
            {
                using (var deflateStream = new DeflateStream(compressedStream, CompressionMode.Compress, true))
                {
                    deflateStream.Write(originalBytes, 0, originalBytes.Length);
                }
                compressedBytes = compressedStream.ToArray();
            }

            var (httpClient, handler) = CreateMockHttpClient(compressedBytes, "deflate");
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings { EnableCompression = true };
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

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
            var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

            connection.Dispose();
            // Double dispose should not throw
            connection.Dispose();
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
            serializer.SerializeMessageAsync(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new byte[] { 0x84 }));
            serializer.DeserializeMessageAsync(Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(
                    new ResponseMessage<List<object>>(false, results, 200, null, null)));
            return serializer;
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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient, interceptors);

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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient, interceptors);

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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient, interceptors);

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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient, interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.Equal("first", observedHeader);
            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("second",
                handler.CapturedRequest!.Headers.GetValues("X-Custom").First());
        }

        [Fact]
        public async Task ShouldSerializeBeforeInterceptorsWhenRequestSerializerProvided()
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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.IsType<byte[]>(observedBody);
        }

        [Fact]
        public async Task ShouldPassRequestMessageWhenRequestSerializerIsNull()
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
                    // Serialize the body so the request can proceed
                    ctx.Body = new byte[] { 0x84 };
                    ctx.Headers["Content-Type"] = "application/vnd.graphbinary-v4.0";
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, null, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.IsType<RequestMessage>(observedBody);
        }

        [Fact]
        public async Task ShouldThrowWhenBodyIsNotByteArrayAfterInterceptors()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            // No interceptor serializes the body
            var interceptors = new List<Func<HttpRequestContext, Task>>();

            using var connection = new Connection(TestUri, null, serializer, settings, httpClient,
                interceptors);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => connection.SubmitAsync<object>(CreateTestRequest()));

            Assert.Contains("byte[] or HttpContent", ex.Message);
            Assert.Contains("RequestMessage", ex.Message);
            // HTTP request should not have been sent
            Assert.Null(handler.CapturedRequest);
        }

        [Fact]
        public async Task ShouldSucceedWhenInterceptorSerializesBodyWithNullRequestSerializer()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                async ctx =>
                {
                    if (ctx.Body is RequestMessage msg)
                    {
                        ctx.Body = await serializer.SerializeMessageAsync(msg);
                        ctx.Headers["Content-Type"] = "application/vnd.graphbinary-v4.0";
                    }
                },
            };

            using var connection = new Connection(TestUri, null, serializer, settings, httpClient,
                interceptors);

            var result = await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(result);
            Assert.NotNull(handler.CapturedRequest);
        }

        [Fact]
        public async Task ShouldNotSetContentTypeWhenRequestSerializerIsNull()
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
                    // Serialize so the request can proceed
                    ctx.Body = new byte[] { 0x84 };
                    ctx.Headers["Content-Type"] = "application/vnd.graphbinary-v4.0";
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, null, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.False(hadContentType, "Content-Type should not be set before interceptors when requestSerializer is null");
            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("application/vnd.graphbinary-v4.0",
                handler.CapturedRequest!.Content!.Headers.ContentType!.MediaType);
        }

        [Fact]
        public async Task ShouldWorkWithEmptyInterceptorList()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            var interceptors = new List<Func<HttpRequestContext, Task>>();

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.True(interceptorCompleted);
            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("async-value",
                handler.CapturedRequest!.Headers.GetValues("X-Async-Header").First());
        }

        [Fact]
        public async Task ShouldAllowInterceptorToReadSerializedBody()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();
            byte[]? capturedBody = null;

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    capturedBody = ctx.Body as byte[];
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(capturedBody);
            // The mock serializer returns { 0x84 }
            Assert.Equal(new byte[] { 0x84 }, capturedBody);
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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
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

            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("one", handler.CapturedRequest!.Headers.GetValues("X-First").First());
            Assert.Equal("two", handler.CapturedRequest.Headers.GetValues("X-Second").First());
            Assert.Equal("three", handler.CapturedRequest.Headers.GetValues("X-Third").First());
        }

        [Fact]
        public async Task ShouldAllowCustomContentTypeWhenRequestSerializerIsNull()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var serializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                ctx =>
                {
                    ctx.Body = new byte[] { 0x01 };
                    ctx.Headers["Content-Type"] = "application/json";
                    return Task.CompletedTask;
                },
            };

            using var connection = new Connection(TestUri, null, serializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("application/json",
                handler.CapturedRequest!.Content!.Headers.ContentType!.MediaType);
        }

        [Fact]
        public async Task ShouldUseResponseSerializerWhenRequestSerializerIsNull()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var requestSerializer = CreateMockSerializer();
            var responseSerializer = CreateMockSerializer();
            var settings = new ConnectionSettings();

            var interceptors = new List<Func<HttpRequestContext, Task>>
            {
                async ctx =>
                {
                    if (ctx.Body is RequestMessage msg)
                    {
                        ctx.Body = await requestSerializer.SerializeMessageAsync(msg);
                        ctx.Headers["Content-Type"] = "application/vnd.graphbinary-v4.0";
                    }
                },
            };

            using var connection = new Connection(TestUri, null, responseSerializer, settings, httpClient,
                interceptors);

            await connection.SubmitAsync<object>(CreateTestRequest());

            // Verify the response serializer was called for deserialization
            await responseSerializer.Received(1)
                .DeserializeMessageAsync(Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
            // Verify the request serializer was NOT called by Connection (interceptor called it directly)
            await requestSerializer.DidNotReceive()
                .DeserializeMessageAsync(Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
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

            using var connection = new Connection(TestUri, null, serializer, settings, httpClient,
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
            var requestSerializer = CreateMockSerializer("application/custom-request");
            var responseSerializer = CreateMockSerializer("application/custom-response");
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, requestSerializer, responseSerializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Contains(handler.CapturedRequest!.Headers.Accept,
                h => h.MediaType == "application/custom-response");
        }

        [Fact]
        public async Task ShouldUseRequestSerializerMimeTypeForContentTypeHeader()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var requestSerializer = CreateMockSerializer("application/custom-request");
            var responseSerializer = CreateMockSerializer("application/custom-response");
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, requestSerializer, responseSerializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            Assert.Equal("application/custom-request",
                handler.CapturedRequest!.Content!.Headers.ContentType!.MediaType);
        }

        [Fact]
        public async Task ShouldUseDifferentMimeTypesForRequestAndResponseSerializers()
        {
            var (httpClient, handler) = CreateMockHttpClient();
            var requestSerializer = CreateMockSerializer("application/vnd.custom-request-v1.0");
            var responseSerializer = CreateMockSerializer("application/vnd.custom-response-v2.0");
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, requestSerializer, responseSerializer, settings, httpClient);

            await connection.SubmitAsync<object>(CreateTestRequest());

            Assert.NotNull(handler.CapturedRequest);
            // Content-Type comes from request serializer
            Assert.Equal("application/vnd.custom-request-v1.0",
                handler.CapturedRequest!.Content!.Headers.ContentType!.MediaType);
            // Accept comes from response serializer
            Assert.Contains(handler.CapturedRequest.Headers.Accept,
                h => h.MediaType == "application/vnd.custom-response-v2.0");
        }

        [Fact]
        public async Task ShouldBoxNonTraverserResultsIntoTraversers()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var serializer = CreateMockSerializer(new List<object> { "hello", 42 });
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

            var result = await connection.SubmitAsync<Traverser>(CreateTestRequest());

            var items = result.ToList();
            Assert.Equal(2, items.Count);
            Assert.Equal("hello", items[0].Object);
            Assert.Equal(1, items[0].Bulk);
            Assert.Equal(42, items[1].Object);
            Assert.Equal(1, items[1].Bulk);
        }

        [Fact]
        public async Task ShouldNotDoubleBoxTraverserResults()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var serializer = CreateMockSerializer(new List<object> { new Traverser("existing", 5) });
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

            var result = await connection.SubmitAsync<Traverser>(CreateTestRequest());

            var item = result.Single();
            Assert.Equal("existing", item.Object);
            Assert.Equal(5, item.Bulk);
        }

        [Fact]
        public async Task ShouldBoxMixedTraverserAndNonTraverserResults()
        {
            var (httpClient, _) = CreateMockHttpClient();
            var serializer = CreateMockSerializer(new List<object> { new Traverser("already", 3), "raw" });
            var settings = new ConnectionSettings();
            using var connection = new Connection(TestUri, serializer, serializer, settings, httpClient);

            var result = await connection.SubmitAsync<Traverser>(CreateTestRequest());

            var items = result.ToList();
            Assert.Equal(2, items.Count);
            Assert.Equal("already", items[0].Object);
            Assert.Equal(3, items[0].Bulk);
            Assert.Equal("raw", items[1].Object);
            Assert.Equal(1, items[1].Bulk);
        }

        /// <summary>
        ///     A test HttpMessageHandler that captures the request and returns a canned response.
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
    }
}
