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
using System.Net;
using System.Net.Security;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Holds settings for the HTTP connection.
    /// </summary>
    public class ConnectionSettings
    {
        /// <summary>
        ///     The default TCP connect timeout (transport establishment, i.e. TCP connect plus
        ///     TLS handshake where applicable - not an HTTP request timeout).
        /// </summary>
        public static readonly TimeSpan DefaultConnectTimeout = TimeSpan.FromSeconds(5);

        /// <summary>
        ///     The default idle connection timeout.
        /// </summary>
        public static readonly TimeSpan DefaultIdleTimeout = TimeSpan.FromSeconds(180);

        /// <summary>
        ///     The default maximum concurrent connections to a single server.
        /// </summary>
        public const int DefaultMaxConnections = 128;

        /// <summary>
        ///     The default TCP keep-alive idle time (how long a connection is idle before the
        ///     first keep-alive probe is sent).
        /// </summary>
        public static readonly TimeSpan DefaultKeepAliveTime = TimeSpan.FromSeconds(30);

        /// <summary>
        ///     The default batch size used to fill the per-request batch size when it is unset.
        /// </summary>
        public const int DefaultBatchSizeValue = 64;

        /// <summary>
        ///     Gets or sets the TCP connect timeout. This is a transport-establishment timeout
        ///     (TCP connect plus TLS handshake where applicable), not an HTTP request timeout.
        /// </summary>
        public TimeSpan ConnectTimeout { get; set; } = DefaultConnectTimeout;

        /// <summary>
        ///     Gets or sets <see cref="ConnectTimeout"/> in whole milliseconds. This is the millisecond
        ///     view of the same setting; <see cref="ConnectTimeout"/> is the idiomatic <see cref="TimeSpan"/> form.
        /// </summary>
        public int ConnectTimeoutMillis
        {
            get => (int) ConnectTimeout.TotalMilliseconds;
            set => ConnectTimeout = TimeSpan.FromMilliseconds(value);
        }

        /// <summary>
        ///     Gets or sets how long idle connections stay in the pool before being closed.
        /// </summary>
        public TimeSpan IdleTimeout { get; set; } = DefaultIdleTimeout;

        /// <summary>
        ///     Gets or sets <see cref="IdleTimeout"/> in whole milliseconds. This is the millisecond
        ///     view of the same setting; <see cref="IdleTimeout"/> is the idiomatic <see cref="TimeSpan"/> form.
        /// </summary>
        public int IdleTimeoutMillis
        {
            get => (int) IdleTimeout.TotalMilliseconds;
            set => IdleTimeout = TimeSpan.FromMilliseconds(value);
        }

        /// <summary>
        ///     Gets or sets the maximum concurrent connections to a single server.
        /// </summary>
        public int MaxConnections { get; set; } = DefaultMaxConnections;

        /// <summary>
        ///     Gets or sets the TCP keep-alive idle time: how long a connection may be idle
        ///     before the first keep-alive probe is sent. Probe interval and count stay at OS
        ///     defaults.
        /// </summary>
        public TimeSpan KeepAliveTime { get; set; } = DefaultKeepAliveTime;

        /// <summary>
        ///     Gets or sets <see cref="KeepAliveTime"/> in whole milliseconds. This is the millisecond
        ///     view of the same setting; <see cref="KeepAliveTime"/> is the idiomatic <see cref="TimeSpan"/> form.
        /// </summary>
        public int KeepAliveTimeMillis
        {
            get => (int) KeepAliveTime.TotalMilliseconds;
            set => KeepAliveTime = TimeSpan.FromMilliseconds(value);
        }

        /// <summary>
        ///     Gets or sets the response compression algorithm. Defaults to
        ///     <see cref="Driver.Compression.Deflate"/> (on); set <see cref="Driver.Compression.None"/>
        ///     to disable.
        /// </summary>
        public Compression Compression { get; set; } = Compression.Deflate;

        /// <summary>
        ///     Gets or sets whether to send the User-Agent header.
        /// </summary>
        public bool EnableUserAgentOnConnect { get; set; } = true;

        /// <summary>
        ///     Gets or sets whether to send the bulkResults: true header on all requests.
        /// </summary>
        public bool BulkResults { get; set; } = false;

        /// <summary>
        ///     Gets or sets the connection-level default batch size that fills the per-request batch size
        ///     when it is not set on the request (client-side default-filling; no wire change).
        /// </summary>
        public int BatchSize { get; set; } = DefaultBatchSizeValue;

        /// <summary>
        ///     Gets or sets the SSL/TLS options used for HTTPS connections (client certificates,
        ///     custom CA, protocols, etc.). When set, these options are used to configure the
        ///     underlying handler. <see cref="SkipCertificateValidation"/> is applied to an
        ///     internal copy of these options rather than mutating the object provided here.
        /// </summary>
        public SslClientAuthenticationOptions? Ssl { get; set; }

        /// <summary>
        ///     Gets or sets whether to skip SSL certificate validation.
        ///     Only use for testing with self-signed certificates. When <see cref="Ssl"/> is also
        ///     provided, the accept-all callback is set on an internal copy of those options so the
        ///     caller's <see cref="SslClientAuthenticationOptions"/> instance is never mutated
        ///     (which is important when one instance is shared across multiple clients).
        /// </summary>
        public bool SkipCertificateValidation { get; set; } = false;

        /// <summary>
        ///     Gets or sets the maximum allowed size, in bytes, of the response headers.
        ///     A value of <c>0</c> (the default) leaves the handler default unchanged. The native
        ///     handler unit is kilobytes; the byte value provided here is converted internally.
        /// </summary>
        public int MaxResponseHeaderBytes { get; set; } = 0;

        /// <summary>
        ///     Gets or sets the read timeout. It bounds two waits: the wait for the initial server
        ///     response (time to first byte / response headers), as a single deadline armed when the
        ///     request is sent; and, once the response body is streaming, the idle time before each
        ///     individual read of the stream, reset per read. It is therefore not a whole-request
        ///     deadline and does not bound the total streaming duration once the server has begun
        ///     responding.
        /// </summary>
        public TimeSpan ReadTimeout { get; set; } = System.Threading.Timeout.InfiniteTimeSpan;

        /// <summary>
        ///     Gets or sets <see cref="ReadTimeout"/> in whole milliseconds, where <c>0</c> disables it
        ///     (mapping to <see cref="System.Threading.Timeout.InfiniteTimeSpan"/>). This is the millisecond
        ///     view of the same setting; <see cref="ReadTimeout"/> is the idiomatic <see cref="TimeSpan"/> form.
        ///     It bounds the wait for the initial server response (time to first byte / response
        ///     headers) as well as the idle time between response body chunks, but it is not a
        ///     whole-request deadline.
        /// </summary>
        public int ReadTimeoutMillis
        {
            get => ReadTimeout <= TimeSpan.Zero ? 0 : (int) ReadTimeout.TotalMilliseconds;
            set => ReadTimeout = value <= 0 ? System.Threading.Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(value);
        }

        /// <summary>
        ///     Gets or sets the HTTP proxy used for connections. When set, it is applied to the
        ///     underlying handler explicitly.
        /// </summary>
        public IWebProxy? Proxy { get; set; }

    }
}
