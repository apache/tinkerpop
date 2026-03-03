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

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Holds settings for the HTTP connection.
    /// </summary>
    public class HttpConnectionSettings
    {
        /// <summary>
        ///     The default TCP connection timeout.
        /// </summary>
        public static readonly TimeSpan DefaultConnectionTimeout = TimeSpan.FromSeconds(15);

        /// <summary>
        ///     The default idle connection timeout.
        /// </summary>
        public static readonly TimeSpan DefaultIdleConnectionTimeout = TimeSpan.FromSeconds(180);

        /// <summary>
        ///     The default maximum connections per server.
        /// </summary>
        public const int DefaultMaxConnectionsPerServer = 128;

        /// <summary>
        ///     The default TCP keep-alive probe interval.
        /// </summary>
        public static readonly TimeSpan DefaultKeepAliveInterval = TimeSpan.FromSeconds(30);

        /// <summary>
        ///     Gets or sets the TCP connection timeout.
        /// </summary>
        public TimeSpan ConnectionTimeout { get; set; } = DefaultConnectionTimeout;

        /// <summary>
        ///     Gets or sets how long idle connections stay in the pool before being closed.
        /// </summary>
        public TimeSpan IdleConnectionTimeout { get; set; } = DefaultIdleConnectionTimeout;

        /// <summary>
        ///     Gets or sets the maximum concurrent connections to a single server.
        /// </summary>
        public int MaxConnectionsPerServer { get; set; } = DefaultMaxConnectionsPerServer;

        /// <summary>
        ///     Gets or sets the TCP keep-alive probe interval.
        /// </summary>
        public TimeSpan KeepAliveInterval { get; set; } = DefaultKeepAliveInterval;

        /// <summary>
        ///     Gets or sets whether to request deflate compression.
        /// </summary>
        public bool EnableCompression { get; set; } = false;

        /// <summary>
        ///     Gets or sets whether to send the User-Agent header.
        /// </summary>
        public bool EnableUserAgentOnConnect { get; set; } = true;

        /// <summary>
        ///     Gets or sets whether to send the bulkResults: true header on all requests.
        /// </summary>
        public bool BulkResults { get; set; } = false;
    }
}
