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
    ///     Holds settings for the <see cref="ConnectionPool"/>.
    /// </summary>
    public class ConnectionPoolSettings
    {
        private const int DefaultMinPoolSize = 8;
        private const int DefaultMaxPoolSize = 128;
        private static readonly TimeSpan DefaultWaitForConnectionTimeout = TimeSpan.FromSeconds(3);

        /// <summary>
        ///     Gets or sets the minimum size of a connection pool.
        /// </summary>
        /// <remarks>The default value is 8.</remarks>
        public int MinSize { get; set; } = DefaultMinPoolSize;

        /// <summary>
        ///     Gets or sets the maximum size of a connection pool.
        /// </summary>
        /// <remarks>The default value is 128.</remarks>
        public int MaxSize { get; set; } = DefaultMaxPoolSize;

        /// <summary>
        ///     Gets or sets the timespan to wait for a new connection before timing out.
        /// </summary>
        /// <remarks>The default value is 3 seconds.</remarks>
        public TimeSpan WaitForConnectionTimeout { get; set; } = DefaultWaitForConnectionTimeout;
    }
}