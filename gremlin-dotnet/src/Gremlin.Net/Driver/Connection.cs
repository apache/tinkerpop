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
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     HTTP-based connection that sends requests via HTTP POST to Gremlin Server.
    /// </summary>
    internal class Connection : IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly Uri _uri;
        private readonly IMessageSerializer _responseSerializer;
        private readonly ConnectionSettings _settings;
        private readonly IReadOnlyList<Func<HttpRequestContext, Task>> _interceptors;

        /// <summary>
        ///     Creates a new HTTP connection. The <see cref="HttpClient"/> is backed by
        ///     SocketsHttpHandler which manages its own TCP connection pool internally,
        ///     so a single <see cref="Connection"/> instance handles concurrent requests efficiently.
        /// </summary>
        /// <param name="uri">The Gremlin Server URI.</param>
        /// <param name="responseSerializer">The serializer for incoming responses (always required).</param>
        /// <param name="settings">Connection settings.</param>
        /// <param name="interceptors">Optional request interceptors.</param>
        public Connection(Uri uri,
            IMessageSerializer responseSerializer,
            ConnectionSettings settings,
            IReadOnlyList<Func<HttpRequestContext, Task>>? interceptors = null)
        {
            _uri = uri;
            _responseSerializer = responseSerializer;
            _settings = settings;
            _interceptors = interceptors ?? Array.Empty<Func<HttpRequestContext, Task>>();

            var handler = new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = settings.IdleTimeout,
                MaxConnectionsPerServer = settings.MaxConnections,
                ConnectTimeout = settings.ConnectTimeout,
            };

            // Rewire keep-alive to a real TCP socket option (HTTP/1.1). The handler's
            // KeepAlivePingTimeout only applies to HTTP/2; instead open the socket ourselves
            // in a ConnectCallback and set the TCP keep-alive idle time. Probe interval and
            // count stay at OS defaults (not standardized).
            var keepAliveTime = settings.KeepAliveTime;
            handler.ConnectCallback = async (context, cancellationToken) =>
            {
                // Resolve the endpoint to concrete IP addresses and attempt each with its own
                // socket. A single Socket cannot be reused across connection attempts (handing a
                // multi-address DnsEndPoint to one socket throws "Sockets on this platform are
                // invalid for use after a failed connection attempt"), so a fresh socket per
                // address is required to support round-robin/fallback DNS.
                var endpoint = context.DnsEndPoint;
                System.Net.IPAddress[] addresses;
                if (System.Net.IPAddress.TryParse(endpoint.Host, out var literal))
                {
                    addresses = new[] { literal };
                }
                else
                {
                    addresses = await System.Net.Dns.GetHostAddressesAsync(
                        endpoint.Host, cancellationToken).ConfigureAwait(false);
                }

                if (addresses.Length == 0)
                {
                    throw new System.Net.Sockets.SocketException(
                        (int)System.Net.Sockets.SocketError.HostNotFound);
                }

                System.Exception? lastError = null;
                foreach (var address in addresses)
                {
                    var socket = new System.Net.Sockets.Socket(
                        address.AddressFamily,
                        System.Net.Sockets.SocketType.Stream,
                        System.Net.Sockets.ProtocolType.Tcp)
                    {
                        NoDelay = true
                    };
                    try
                    {
                        socket.SetSocketOption(System.Net.Sockets.SocketOptionLevel.Socket,
                            System.Net.Sockets.SocketOptionName.KeepAlive, true);
                        var keepAliveSeconds = (int)keepAliveTime.TotalSeconds;
                        if (keepAliveSeconds > 0)
                        {
                            // Set the idle time before the first keep-alive probe. Windows/Linux use
                            // the TcpKeepAliveTime enum; macOS uses the equivalent raw TCP_KEEPALIVE
                            // option. Other platforms keep the OS default idle time.
                            if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux())
                            {
                                socket.SetSocketOption(System.Net.Sockets.SocketOptionLevel.Tcp,
                                    System.Net.Sockets.SocketOptionName.TcpKeepAliveTime, keepAliveSeconds);
                            }
                            else if (OperatingSystem.IsMacOS())
                            {
                                // TCP_KEEPALIVE on macOS (<sys/socket.h>: 0x10) is the idle-time knob,
                                // the analog of Linux TCP_KEEPIDLE.
                                const int tcpKeepAliveMacOs = 0x10;
                                socket.SetSocketOption(System.Net.Sockets.SocketOptionLevel.Tcp,
                                    (System.Net.Sockets.SocketOptionName)tcpKeepAliveMacOs, keepAliveSeconds);
                            }
                        }
                        await socket.ConnectAsync(
                            new System.Net.IPEndPoint(address, endpoint.Port), cancellationToken)
                            .ConfigureAwait(false);
                        return new System.Net.Sockets.NetworkStream(socket, ownsSocket: true);
                    }
                    catch (System.Exception ex)
                    {
                        socket.Dispose();
                        lastError = ex;
                    }
                }

                throw lastError ?? new System.Net.Sockets.SocketException(
                    (int)System.Net.Sockets.SocketError.HostUnreachable);
            };

            // Configure SSL/TLS. Start from the user-supplied options (if any) so client
            // certificates, custom CAs, and protocol settings are preserved. When
            // SkipCertificateValidation is set we must NOT mutate the caller's options object
            // (it is a reference type that may be shared across clients); instead we clone it
            // and install the accept-all callback on the copy.
            if (settings.Ssl != null || settings.SkipCertificateValidation)
            {
                System.Net.Security.SslClientAuthenticationOptions sslOptions;
                if (settings.SkipCertificateValidation)
                {
                    sslOptions = CloneSslOptions(settings.Ssl);
                    sslOptions.RemoteCertificateValidationCallback = (_, _, _, _) => true;
                }
                else
                {
                    sslOptions = settings.Ssl!;
                }
                handler.SslOptions = sslOptions;
            }

            // Expose the max response header size. The native handler unit is kilobytes while
            // the user provides bytes, so convert (rounding up to avoid silently lowering the cap).
            if (settings.MaxResponseHeaderBytes > 0)
            {
                handler.MaxResponseHeadersLength =
                    MaxResponseHeaderBytesToKilobytes(settings.MaxResponseHeaderBytes);
            }

            if (settings.Proxy != null)
            {
                handler.Proxy = settings.Proxy;
                handler.UseProxy = true;
            }

            _httpClient = new HttpClient(handler);
        }

        /// <summary>
        ///     Constructor that accepts a pre-configured HttpClient (for testing).
        /// </summary>
        internal Connection(Uri uri,
            IMessageSerializer responseSerializer,
            ConnectionSettings settings, HttpClient httpClient,
            IReadOnlyList<Func<HttpRequestContext, Task>>? interceptors = null)
        {
            _uri = uri;
            _responseSerializer = responseSerializer;
            _settings = settings;
            _httpClient = httpClient;
            _interceptors = interceptors ?? Array.Empty<Func<HttpRequestContext, Task>>();
        }

        /// <summary>
        ///     Submits a <see cref="RequestMessage"/> to the server and returns a streaming
        ///     <see cref="ResultSet{T}"/> whose background task owns the HTTP response lifetime.
        /// </summary>
        /// <typeparam name="T">The type of the expected result elements.</typeparam>
        /// <param name="requestMessage">The request to send.</param>
        /// <param name="cancellationToken">The token to cancel the operation.</param>
        /// <returns>A <see cref="ResultSet{T}"/> that streams results as they arrive.</returns>
        public async Task<ResultSet<T>> SubmitAsync<T>(RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            var headers = new Dictionary<string, string>();
            headers["Accept"] = _responseSerializer.MimeType;

            // Fill the per-request batch size from the connection-level default when the
            // request did not set one. Build a copy for the outgoing request so the caller's
            // RequestMessage is never mutated (resubmitting the same message must not pick up
            // a previously injected default). A per-request explicit batchSize always wins.
            var outgoingMessage = requestMessage;
            if (!outgoingMessage.Fields.ContainsKey(Tokens.ArgsBatchSize))
            {
                outgoingMessage = outgoingMessage.CloneWithField(
                    Tokens.ArgsBatchSize, _settings.BatchSize);
            }

            if (_settings.Compression.Type == CompressionType.Deflate)
            {
                headers["Accept-Encoding"] = "deflate";
            }

            if (_settings.EnableUserAgentOnConnect)
            {
                headers["User-Agent"] = Utils.UserAgent;
            }

            if (_settings.BulkResults)
            {
                headers["bulkResults"] = "true";
            }

            // Promote transactionId to HTTP header before interceptors run.
            // The field remains in the serialized body as well (dual transmission
            // per the HTTP transaction protocol specification).
            if (outgoingMessage.Fields.TryGetValue(Tokens.ArgsTransactionId, out var txIdObj) &&
                txIdObj is string txId && !string.IsNullOrEmpty(txId))
            {
                headers["X-Transaction-Id"] = txId;
            }

            var context = new HttpRequestContext("POST", _uri, headers, outgoingMessage);

            foreach (var interceptor in _interceptors)
            {
                await interceptor(context).ConfigureAwait(false);
            }

            // Auto-serialize after interceptors: idempotent if already serialized by an interceptor.
            // Skip if body is HttpContent (an escape hatch for full wire-format control).
            if (context.Body is not System.Net.Http.HttpContent)
            {
                context.SerializeBody();
            }

            // The HttpResponseMessage is NOT disposed here — ownership transfers to
            // StreamingResponseContext via the background task.
            HttpResponseMessage response;
            using (var httpRequest = new HttpRequestMessage(new HttpMethod(context.Method), context.Uri))
            {
                if (context.Body is byte[] bodyBytes)
                {
                    httpRequest.Content = new ByteArrayContent(bodyBytes);
                }
                else if (context.Body is HttpContent httpContent)
                {
                    httpRequest.Content = httpContent;
                }
                else
                {
                    throw new InvalidOperationException(
                        "Request body must be byte[] or HttpContent after serialization, " +
                        "but found " + (context.Body?.GetType().Name ?? "null") + ".");
                }

                foreach (var header in context.Headers)
                {
                    if (string.Equals(header.Key, "Content-Type", StringComparison.OrdinalIgnoreCase))
                    {
                        httpRequest.Content.Headers.ContentType = new MediaTypeHeaderValue(header.Value);
                    }
                    else if (string.Equals(header.Key, "Content-Length", StringComparison.OrdinalIgnoreCase))
                    {
                        // Content-Length is set automatically by ByteArrayContent; skip to avoid conflict.
                    }
                    else
                    {
                        httpRequest.Headers.TryAddWithoutValidation(header.Key, header.Value);
                    }
                }

                response = await SendWithReadTimeoutAsync(httpRequest, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (!response.IsSuccessStatusCode &&
                response.Content.Headers.ContentType?.MediaType != _responseSerializer.MimeType)
            {
                using (response)
                {
                    var errorBody = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

                    // Try to extract the "message" field from a JSON error response
                    var errorMessage = TryExtractJsonError(errorBody)
                        ?? $"Gremlin Server returned HTTP {(int)response.StatusCode}: {errorBody}";

                    throw new HttpRequestException(errorMessage);
                }
            }

            StreamingResponseContext? streamingContext = null;
            CancellationTokenSource? disposeCts = null;
            CancellationTokenSource? linkedCts = null;
            try
            {
                var contentStream = await response.Content.ReadAsStreamAsync()
                    .ConfigureAwait(false);

                // Apply the per-read idle timeout (if configured) to the raw content stream so it
                // covers both the compressed and decompressed read paths.
                if (_settings.ReadTimeout > TimeSpan.Zero)
                {
                    contentStream = new ReadTimeoutStream(contentStream, _settings.ReadTimeout);
                }

                // The server (gremlin-server HttpContentCompressionHandler) compresses with
                // java.util.zip.Deflater's default constructor, which emits a zlib-wrapped
                // stream (RFC 1950: 2-byte header + Adler-32 checksum), not raw DEFLATE
                // (RFC 1951). ZLibStream understands that wrapper; DeflateStream would throw
                // on the zlib header.
                Stream? decompressionStream = null;
                if (response.Content.Headers.ContentEncoding.Contains("deflate"))
                {
                    decompressionStream = new ZLibStream(contentStream, CompressionMode.Decompress);
                }
                streamingContext = new StreamingResponseContext(
                    response, contentStream, decompressionStream);

                var resultStream = _responseSerializer.DeserializeMessageAsync(
                    streamingContext.Stream, cancellationToken);

                var channel = Channel.CreateUnbounded<object>(
                    new UnboundedChannelOptions { SingleWriter = true });
                disposeCts = new CancellationTokenSource();
                linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken, disposeCts.Token);

                var capturedLinkedCts = linkedCts;
                var capturedStreamingContext = streamingContext;

                var backgroundTask = Task.Run(async () =>
                {
                    // Note: ResponseException from the status footer is propagated after
                    // all result items have been yielded, so consumers will see all results
                    // before the exception if the status code is non-200.
                    try
                    {
                        await foreach (var item in resultStream
                            .WithCancellation(capturedLinkedCts.Token).ConfigureAwait(false))
                        {
                            await channel.Writer.WriteAsync(item, capturedLinkedCts.Token)
                                .ConfigureAwait(false);
                        }
                        channel.Writer.Complete();
                    }
                    catch (Exception ex) when (ex is not ResponseException
                                                    and not OperationCanceledException
                                                    and not HttpIOException)
                    {
                        channel.Writer.Complete(
                            new ResponseDeserializationException(ex));
                    }
                    catch (Exception ex)
                    {
                        channel.Writer.Complete(ex);
                    }
                    finally
                    {
                        capturedLinkedCts.Dispose();
                        capturedStreamingContext.Dispose();
                    }
                }, CancellationToken.None);

                // Ownership transferred to background task — prevent catch block from
                // double-disposing.
                linkedCts = null;
                streamingContext = null;

                return new ResultSet<T>(channel.Reader, disposeCts, backgroundTask);
            }
            catch
            {
                linkedCts?.Dispose();
                // If streamingContext was not yet created, the response is not yet owned
                // by it and must be disposed separately.
                if (streamingContext == null)
                {
                    response.Dispose();
                }
                else
                {
                    streamingContext.Dispose();
                }
                disposeCts?.Dispose();
                throw;
            }
        }

        /// <summary>
        ///     Sends the request and waits for the response headers, bounding that wait by
        ///     <see cref="ConnectionSettings.ReadTimeout"/> when it is positive. This uses the same
        ///     disambiguation idiom as <see cref="ReadTimeoutStream"/>: a timeout
        ///     <see cref="CancellationTokenSource"/> linked with the caller token, armed with
        ///     <see cref="CancellationTokenSource.CancelAfter(TimeSpan)"/>, so a fired timeout
        ///     (when the caller token did not fire) surfaces as a <see cref="TimeoutException"/>.
        ///     When <see cref="ConnectionSettings.ReadTimeout"/> is non-positive the caller token is
        ///     passed straight through with no wrapping. The timeout CTS is disposed once headers are
        ///     read so its timer does not linger into body streaming.
        /// </summary>
        private async Task<HttpResponseMessage> SendWithReadTimeoutAsync(HttpRequestMessage httpRequest,
            CancellationToken cancellationToken)
        {
            if (_settings.ReadTimeout <= TimeSpan.Zero)
            {
                return await _httpClient.SendAsync(httpRequest,
                    HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                    .ConfigureAwait(false);
            }

            using var timeoutCts = new CancellationTokenSource();
            using var linkedCts =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
            timeoutCts.CancelAfter(_settings.ReadTimeout);
            try
            {
                return await _httpClient.SendAsync(httpRequest,
                    HttpCompletionOption.ResponseHeadersRead, linkedCts.Token)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested &&
                                                     !cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException(
                    $"Timed out after {_settings.ReadTimeout.TotalSeconds:0.###}s waiting for the initial server response.");
            }
        }

        /// <summary>
        ///     Converts a maximum response header size expressed in bytes to the kilobyte unit
        ///     used by <see cref="SocketsHttpHandler.MaxResponseHeadersLength"/>, rounding up so
        ///     the configured byte cap is never silently lowered. For example 1024 bytes maps to
        ///     1 KB, 1025 bytes maps to 2 KB, and 8192 bytes maps to 8 KB. Callers only invoke
        ///     this when <paramref name="maxResponseHeaderBytes"/> is positive.
        /// </summary>
        /// <param name="maxResponseHeaderBytes">The header cap in bytes (expected to be positive).</param>
        /// <returns>The equivalent cap in kilobytes, rounded up.</returns>
        internal static int MaxResponseHeaderBytesToKilobytes(int maxResponseHeaderBytes)
        {
            return (maxResponseHeaderBytes + 1023) / 1024;
        }

        /// <summary>
        ///     Creates a shallow copy of the supplied
        ///     <see cref="System.Net.Security.SslClientAuthenticationOptions"/> so the caller's
        ///     object is never mutated when the skip-cert convenience is applied. Copies the
        ///     commonly used properties; the accept-all
        ///     <see cref="System.Net.Security.SslClientAuthenticationOptions.RemoteCertificateValidationCallback"/>
        ///     is set on the returned copy by the caller.
        /// </summary>
        /// <param name="source">The caller-owned options to clone, or <c>null</c>.</param>
        /// <returns>A new options instance carrying the copied settings.</returns>
        private static System.Net.Security.SslClientAuthenticationOptions CloneSslOptions(
            System.Net.Security.SslClientAuthenticationOptions? source)
        {
            var clone = new System.Net.Security.SslClientAuthenticationOptions();
            if (source == null)
            {
                return clone;
            }

            clone.ClientCertificates = source.ClientCertificates;
            clone.EnabledSslProtocols = source.EnabledSslProtocols;
            clone.TargetHost = source.TargetHost;
            // RemoteCertificateValidationCallback is intentionally NOT copied here: the caller
            // overwrites it with the accept-all callback (skip-cert is the only path that clones).
            clone.LocalCertificateSelectionCallback = source.LocalCertificateSelectionCallback;
            clone.CipherSuitesPolicy = source.CipherSuitesPolicy;
            clone.EncryptionPolicy = source.EncryptionPolicy;
            clone.ApplicationProtocols = source.ApplicationProtocols;
            clone.CertificateRevocationCheckMode = source.CertificateRevocationCheckMode;
            clone.AllowRenegotiation = source.AllowRenegotiation;
            // ClientCertificateContext carries the mTLS client certificate chain; omitting it
            // would break client-certificate auth when combined with skip-cert.
            clone.ClientCertificateContext = source.ClientCertificateContext;
            // AllowTlsResume defaults to true, so it must be copied to honor a caller's false.
            clone.AllowTlsResume = source.AllowTlsResume;
            return clone;
        }

        /// <summary>
        ///     Attempts to extract an error message from a JSON response body.
        ///     The server sometimes responds with a JSON object containing a "message" field
        ///     even when it cannot produce a GraphBinary response.
        /// </summary>
        private static string? TryExtractJsonError(string body)
        {
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(body);
                if (doc.RootElement.TryGetProperty("message", out var messageProp))
                {
                    return messageProp.GetString();
                }
            }
            catch
            {
                // Not valid JSON — fall through to raw body
            }
            return null;
        }

        #region IDisposable Support

        private bool _disposed;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _httpClient?.Dispose();
                }
                _disposed = true;
            }
        }

        #endregion

        internal IReadOnlyList<Func<HttpRequestContext, Task>> Interceptors => _interceptors;
    }
}
