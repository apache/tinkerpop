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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Gremlin.Net.Driver
{
    internal class ConnectionPool : IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly ConcurrentBag<Connection> _connections = new ConcurrentBag<Connection>();
        private readonly object _connectionsLock = new object();

        public ConnectionPool(ConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        public int NrConnections { get; private set; }

        public async Task<IConnection> GetAvailableConnectionAsync()
        {
            Connection connection = null;
            lock (_connectionsLock)
            {
                if (!_connections.IsEmpty)
                    _connections.TryTake(out connection);
            }

            if (connection == null)
                connection = await CreateNewConnectionAsync().ConfigureAwait(false);

            return new ProxyConnection(connection, AddConnection);
        }

        private async Task<Connection> CreateNewConnectionAsync()
        {
            NrConnections++;
            var newConnection = _connectionFactory.CreateConnection();
            await newConnection.ConnectAsync().ConfigureAwait(false);
            return newConnection;
        }

        private void AddConnection(Connection connection)
        {
            lock (_connectionsLock)
            {
                _connections.Add(connection);
            }
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
                    lock (_connectionsLock)
                    {
                        if (_connections != null && !_connections.IsEmpty)
                        {
                            TeardownAsync().Wait();

                            foreach (var conn in _connections)
                                conn.Dispose();
                        }
                    }
                _disposed = true;
            }
        }

        private async Task TeardownAsync()
        {
            var closeTasks = new List<Task>(_connections.Count);
            closeTasks.AddRange(_connections.Select(conn => conn.CloseAsync()));
            await Task.WhenAll(closeTasks).ConfigureAwait(false);
        }

        #endregion
    }
}