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

namespace Gremlin.Net.Driver.Exceptions
{
    /// <summary>
    ///     The exception that is thrown when all connections in the pool have reached their maximum number of in-flight
    ///     requests.
    /// </summary>
    public class ConnectionPoolBusyException : NoConnectionAvailableException
    {
        /// <summary>
        ///     Gets the size of the connection pool.
        /// </summary>
        public int PoolSize { get; }

        /// <summary>
        ///     Gets the maximum number of in-flight requests that can occur on a connection.
        /// </summary>
        public int MaxInProcessPerConnection { get; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="ConnectionPoolBusyException" /> class.
        /// </summary>
        public ConnectionPoolBusyException(int poolSize, int maxInProcessPerConnection)
            : base(CreateMessage(poolSize, maxInProcessPerConnection))
        {
            PoolSize = poolSize;
            MaxInProcessPerConnection = maxInProcessPerConnection;
        }

        private static string CreateMessage(int poolSize, int maxInProcessPerConnection)
        {
            return $"All {poolSize} connections have reached their " +
                   $"{nameof(ConnectionPoolSettings.MaxInProcessPerConnection)} limit of {maxInProcessPerConnection}." +
                   $" Consider increasing either the {nameof(ConnectionPoolSettings.PoolSize)} or the " +
                   $"{nameof(ConnectionPoolSettings.MaxInProcessPerConnection)} limit.";
        }
    }
}