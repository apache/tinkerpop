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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Process;

namespace Gremlin.Net.Driver
{
    internal class ConnectionPool : IDisposable
    {
        private const int ConnectionIndexOverflowLimit = int.MaxValue - 1000000;

        private readonly ConnectionFactory _connectionFactory;
        private readonly CopyOnWriteCollection<Connection> _connections = new CopyOnWriteCollection<Connection>();
        private readonly int _poolSize;
        private readonly int _maxInProcessPerConnection;
        private int _connectionIndex;
        private int _poolState;
        private const int PoolIdle = 0;
        private const int PoolPopulationInProgress = 1;

        public ConnectionPool(ConnectionFactory connectionFactory, ConnectionPoolSettings settings)
        {
            _connectionFactory = connectionFactory;
            _poolSize = settings.PoolSize;
            _maxInProcessPerConnection = settings.MaxInProcessPerConnection;
            PopulatePoolAsync().WaitUnwrap();
        }

        public int NrConnections => _connections.Count;
        internal Connection FirstConnectionSnapshot
        {
            get
            {
                for (int i = 0; i < _connections.Count; i++)
                {
                    if (_connections != null && _connections.Snapshot != null && _connections.Snapshot[i] != null)
                    {
                        return _connections.Snapshot[i];
                    };
                }
                return null;
            }
        }

        public async Task<IConnection> GetAvailableConnectionAsync()
        {
            await EnsurePoolIsPopulatedAsync().ConfigureAwait(false);
            return ProxiedConnection(GetConnectionFromPool());
        }

        private async Task EnsurePoolIsPopulatedAsync()
        {
            // The pool could have been (partially) empty because of  connection problems. So, we need to populate it again.
            if (_poolSize <= NrConnections) return;
            var poolState = Interlocked.CompareExchange(ref _poolState, PoolPopulationInProgress, PoolIdle);
            if (poolState == PoolPopulationInProgress) return;
            try
            {
                await PopulatePoolAsync().ConfigureAwait(false);
            }
            finally
            {
                // We need to remove the PoolPopulationInProgress flag again even if an exception occurred, so we don't block the pool population for ever
                Interlocked.CompareExchange(ref _poolState, PoolIdle, PoolPopulationInProgress);
            }
        }

        private async Task PopulatePoolAsync()
        {
            var nrConnectionsToCreate = _poolSize - _connections.Count;
            var connectionCreationTasks = new List<Task<Connection>>(nrConnectionsToCreate);
            try
            {
                for (var i = 0; i < nrConnectionsToCreate; i++)
                {
                    connectionCreationTasks.Add(CreateNewConnectionAsync());
                }
                var createdConnections = await Task.WhenAll(connectionCreationTasks).ConfigureAwait(false);
                _connections.AddRange(createdConnections);
            }
            catch (Exception)
            {
                // Dispose created connections if the connection establishment failed
                foreach (var creationTask in connectionCreationTasks)
                {
                    if (!creationTask.IsFaulted)
                        creationTask.Result?.Dispose();
                }
                throw;
            }
        }

        private async Task<Connection> CreateNewConnectionAsync()
        {
            var newConnection = _connectionFactory.CreateConnection();
            await newConnection.ConnectAsync().ConfigureAwait(false);
            return newConnection;
        }

        private Connection GetConnectionFromPool()
        {
            var connections = _connections.Snapshot;
            if (connections.Length == 0) throw new ServerUnavailableException();
            return TryGetAvailableConnection(connections);
        }

        private Connection TryGetAvailableConnection(Connection[] connections)
        {
            var index = Interlocked.Increment(ref _connectionIndex);
            ProtectIndexFromOverflowing(index);

            for (var i = 0; i < connections.Length; i++)
            {
                var connection = connections[(index + i) % connections.Length];
                if (connection.NrRequestsInFlight >= _maxInProcessPerConnection) continue;
                if (!connection.IsOpen)
                {
                    RemoveConnectionFromPool(connection);
                    continue;
                }
                return connection;
            }
            throw new ConnectionPoolBusyException(_poolSize, _maxInProcessPerConnection);
        }

        private void ProtectIndexFromOverflowing(int currentIndex)
        {
            if (currentIndex > ConnectionIndexOverflowLimit)
                Interlocked.Exchange(ref _connectionIndex, 0);
        }

        private void RemoveConnectionFromPool(Connection connection)
        {
            if (_connections.TryRemove(connection))
                DefinitelyDestroyConnection(connection);
        }

        private IConnection ProxiedConnection(Connection connection)
        {
            return new ProxyConnection(connection, ReturnConnectionIfOpen);
        }

        private void ReturnConnectionIfOpen(Connection connection)
        {
            if (connection.IsOpen) return;
            ConsiderUnavailable();
        }

        private void ConsiderUnavailable()
        {
            CloseAndRemoveAllConnectionsAsync().WaitUnwrap();
        }

        private async Task CloseAndRemoveAllConnectionsAsync()
        {
            foreach (var connection in _connections.RemoveAndGetAll())
            {
                await connection.CloseAsync().ConfigureAwait(false);
                DefinitelyDestroyConnection(connection);
            }
        }

        private void DefinitelyDestroyConnection(Connection connection)
        {
            connection.Dispose();
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
                    CloseAndRemoveAllConnectionsAsync().WaitUnwrap();
                _disposed = true;
            }
        }

        #endregion
    }
}