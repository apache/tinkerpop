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
using Gremlin.Net.Driver.Exceptions;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Holds settings for the <see cref="ConnectionPool"/>.
    /// </summary>
    public class ConnectionPoolSettings
    {
        private int _poolSize = DefaultPoolSize;
        private int _maxInProcessPerConnection = DefaultMaxInProcessPerConnection;
        private int _reconnectionAttempts = DefaultReconnectionAttempts;
        private TimeSpan _reconnectionBaseDelay = DefaultReconnectionBaseDelay;
        private const int DefaultPoolSize = 4;
        private const int DefaultMaxInProcessPerConnection = 32;
        private const int DefaultReconnectionAttempts = 4;
        private static readonly TimeSpan DefaultReconnectionBaseDelay = TimeSpan.FromSeconds(1);

        /// <summary>
        ///     Gets or sets the size of the connection pool.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">The specified pool size is less than or equal to zero.</exception>
        /// <remarks>
        ///     The default value is 4.
        /// </remarks>
        public int PoolSize
        {
            get => _poolSize;
            set
            {
                if (value <= 0)
                    throw new ArgumentOutOfRangeException(nameof(PoolSize), $"{nameof(PoolSize)} must be > 0!");
                _poolSize = value;
            }
        }

        /// <summary>
        ///     Gets or sets the maximum number of in-flight requests that can occur on a connection.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">The specified number is less than or equal to zero.</exception>
        /// <remarks>
        ///     The default value is 32. A <see cref="ConnectionPoolBusyException" /> is thrown if this limit is reached
        ///     on all connections when a new request comes in.
        /// </remarks>
        public int MaxInProcessPerConnection
        {
            get => _maxInProcessPerConnection;
            set
            {
                if (value <= 0)
                    throw new ArgumentOutOfRangeException(nameof(MaxInProcessPerConnection),
                        $"{nameof(MaxInProcessPerConnection)} must be > 0!");
                _maxInProcessPerConnection = value;
            }
        }

        /// <summary>
        ///     Gets or sets the number of attempts to get an open connection from the pool to submit a request.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">The number of attempts specified is less than zero.</exception>
        /// <remarks>
        ///     The driver always tries to reconnect to a server in the background after it has noticed that a
        ///     connection is dead. This setting only specifies how often the driver will retry to get an open
        ///     connection from its pool when no open connection is available to submit a request.  
        ///     These retries give the driver time to establish new connections to the server that might have been
        ///     unavailable temporarily or that might have closed the connections, e.g., because they were idle for some
        ///     time.
        /// 
        ///     The default value is 4. A <see cref="ServerUnavailableException" /> is thrown if the server can still
        ///     not be reached after this many retry attempts.
        ///
        ///     Setting this to zero means that the exception is thrown immediately when no open connection is available
        ///     to submit a request. The driver will however still try to reconnect to the server in the background for
        ///     subsequent requests.
        /// </remarks>
        public int ReconnectionAttempts
        {
            get => _reconnectionAttempts;
            set
            {
                if (value < 0)
                    throw new ArgumentOutOfRangeException(nameof(ReconnectionAttempts),
                        $"{ReconnectionAttempts} must be >= 0!");
                _reconnectionAttempts = value;
            }
        }

        /// <summary>
        ///     Gets or sets the base delay used for the exponential backoff for the reconnection attempts.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">The specified delay is negative or zero.</exception>
        /// <remarks>
        ///     The driver employs an exponential backoff strategy that uses this base delay for its reconnection
        ///     attempts. With a base delay of 100 ms for example, retries happen after 100 ms, 200 ms, 400 ms, 800 ms,
        ///     and so on, assuming that enough <see cref="ReconnectionAttempts"/> are configured.
        ///
        ///     The default value is 1 second.
        /// </remarks>
        public TimeSpan ReconnectionBaseDelay
        {
            get => _reconnectionBaseDelay;
            set
            {
                if (value <= TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException(nameof(ReconnectionBaseDelay),
                        $"{nameof(ReconnectionBaseDelay)} must be > 0!");
                _reconnectionBaseDelay = value;
            }
        }
    }
}