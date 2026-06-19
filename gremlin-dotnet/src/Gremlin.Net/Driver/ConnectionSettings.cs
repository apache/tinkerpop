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
        ///     The default maximum concurrent connections to a single server.
        /// </summary>
        /// <remarks>Deprecated compatibility alias; use <see cref="DefaultMaxConnections"/>.</remarks>
        [Obsolete("As of release 4.0.0, replaced by DefaultMaxConnections.")]
        public const int DefaultMaxConnectionsPerServer = DefaultMaxConnections;

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
        ///     Gets or sets how long idle connections stay in the pool before being closed.
        /// </summary>
        public TimeSpan IdleTimeout { get; set; } = DefaultIdleTimeout;

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
        ///     Gets or sets the default batch size that fills the per-request batch size when it
        ///     is not set on the request (client-side default-filling; no wire change).
        /// </summary>
        public int DefaultBatchSize { get; set; } = DefaultBatchSizeValue;

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
        ///     Gets or sets the idle-read timeout applied to each individual read of the response
        ///     stream. It resets per chunk, so it is an idle-read timeout rather than a
        ///     whole-request deadline. <see cref="System.Threading.Timeout.InfiniteTimeSpan"/>
        ///     (the default) disables it.
        /// </summary>
        public TimeSpan ReadTimeout { get; set; } = System.Threading.Timeout.InfiniteTimeSpan;

        /// <summary>
        ///     Gets or sets the HTTP proxy used for connections. When set, it is applied to the
        ///     underlying handler explicitly.
        /// </summary>
        public IWebProxy? Proxy { get; set; }

        /// <summary>
        ///     Gets or sets the maximum concurrent connections to a single server.
        /// </summary>
        /// <remarks>Deprecated compatibility alias; delegates to <see cref="MaxConnections"/>.</remarks>
        [Obsolete("As of release 4.0.0, replaced by MaxConnections.")]
        public int MaxConnectionsPerServer
        {
            get => MaxConnections;
            set => MaxConnections = value;
        }

        /// <summary>
        ///     Gets or sets the TCP connect timeout.
        /// </summary>
        /// <remarks>Deprecated compatibility alias; delegates to <see cref="ConnectTimeout"/>.</remarks>
        [Obsolete("As of release 4.0.0, replaced by ConnectTimeout.")]
        public TimeSpan ConnectionTimeout
        {
            get => ConnectTimeout;
            set => ConnectTimeout = value;
        }

        /// <summary>
        ///     Gets or sets how long idle connections stay in the pool before being closed.
        /// </summary>
        /// <remarks>Deprecated compatibility alias; delegates to <see cref="IdleTimeout"/>.</remarks>
        [Obsolete("As of release 4.0.0, replaced by IdleTimeout.")]
        public TimeSpan IdleConnectionTimeout
        {
            get => IdleTimeout;
            set => IdleTimeout = value;
        }

        /// <summary>
        ///     Gets or sets the TCP keep-alive idle time.
        /// </summary>
        /// <remarks>Deprecated compatibility alias; delegates to <see cref="KeepAliveTime"/>.</remarks>
        [Obsolete("As of release 4.0.0, replaced by KeepAliveTime.")]
        public TimeSpan KeepAliveInterval
        {
            get => KeepAliveTime;
            set => KeepAliveTime = value;
        }

        /// <summary>
        ///     Gets or sets whether response compression is enabled.
        /// </summary>
        /// <remarks>
        ///     Deprecated compatibility shim; delegates to <see cref="Compression"/>
        ///     (<c>true</c> = <see cref="Driver.Compression.Deflate"/>,
        ///     <c>false</c> = <see cref="Driver.Compression.None"/>).
        /// </remarks>
        [Obsolete("As of release 4.0.0, replaced by Compression.")]
        public bool EnableCompression
        {
            get => Compression.Type == CompressionType.Deflate;
            set => Compression = value ? Compression.Deflate : Compression.None;
        }
    }
}
