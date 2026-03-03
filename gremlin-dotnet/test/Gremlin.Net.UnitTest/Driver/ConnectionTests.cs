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
            ms.WriteByte(0x81); // version
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
            Assert.Equal(SerializationTokens.GraphBinary4MimeType,
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
            var settings = new ConnectionSettings { EnableCompression = true };
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
            var settings = new ConnectionSettings { EnableCompression = false };
            using var connection = new Connection(TestUri, serializer, settings, httpClient);

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

        private static RequestMessage CreateTestRequest()
        {
            return RequestMessage.Build("g.V()").AddG("g").Create();
        }

        private static IMessageSerializer CreateMockSerializer()
        {
            var serializer = Substitute.For<IMessageSerializer>();
            serializer.SerializeMessageAsync(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new byte[] { 0x81 }));
            serializer.DeserializeMessageAsync(Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(
                    new ResponseMessage<List<object>>(false, new List<object>(), 200, null, null)));
            return serializer;
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
