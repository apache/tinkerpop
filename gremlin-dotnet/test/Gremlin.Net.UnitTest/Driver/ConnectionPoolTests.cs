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
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Moq;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class ConnectionPoolTests
    {
        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(10)]
        public void ShouldEstablishConfiguredNrConnections(int poolSize)
        {
            var mockedConnectionFactory = new Mock<IConnectionFactory>();
            var mockedConnection = new Mock<IConnection>();
            mockedConnectionFactory.Setup(m => m.CreateConnection()).Returns(mockedConnection.Object);
            var pool = CreateConnectionPool(mockedConnectionFactory.Object, poolSize);
            
            Assert.Equal(poolSize, pool.NrConnections);
            mockedConnectionFactory.Verify(m => m.CreateConnection(), Times.Exactly(poolSize));
            mockedConnection.Verify(m => m.ConnectAsync(), Times.Exactly(poolSize));
        }

        [Fact]
        public void GetAvailableConnectionShouldReturnFirstOpenConnection()
        {
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            var openConnectionToReturn = OpenConnection;
            fakeConnectionFactory.SetupSequence(m => m.CreateConnection()).Returns(ClosedConnection)
                .Returns(ClosedConnection).Returns(openConnectionToReturn);
            var pool = CreateConnectionPool(fakeConnectionFactory.Object, 3);

            var returnedConnection = pool.GetAvailableConnection();

            Assert.Equal(openConnectionToReturn, ((ProxyConnection) returnedConnection).ProxiedConnection);
        }
        
        [Fact]
        public void GetAvailableConnectionShouldThrowIfAllConnectionsAreClosed()
        {
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory.Object);

            Assert.Throws<ServerUnavailableException>(() => pool.GetAvailableConnection());
        }
        
        [Fact]
        public void GetAvailableConnectionShouldReplaceClosedConnections()
        {
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            fakeConnectionFactory.SetupSequence(m => m.CreateConnection()).Returns(ClosedConnection)
                .Returns(ClosedConnection).Returns(OpenConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory.Object, 3);
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(OpenConnection);
            var nrCreatedConnections = pool.NrConnections;
            
            pool.GetAvailableConnection();
            pool.GetAvailableConnection();
            pool.GetAvailableConnection();

            AssertNrOpenConnections(pool, nrCreatedConnections);
        }

        private static void AssertNrOpenConnections(ConnectionPool connectionPool, int expectedNrConnections)
        {
            for (var i = 0; i < expectedNrConnections; i++)
            {
                var connection = connectionPool.GetAvailableConnection();
                Assert.True(connection.IsOpen);
            }
            Assert.Equal(expectedNrConnections, connectionPool.NrConnections);
        }
        
        [Fact]
        public async Task ShouldNotCreateMoreConnectionsThanConfiguredForParallelRequests()
        {
            var mockedConnectionFactory = new Mock<IConnectionFactory>();
            mockedConnectionFactory.SetupSequence(m => m.CreateConnection()).Returns(ClosedConnection)
                .Returns(ClosedConnection).Returns(OpenConnection);
            var pool = CreateConnectionPool(mockedConnectionFactory.Object, 3);
            mockedConnectionFactory.Setup(m => m.CreateConnection()).Returns(OpenConnection);
            var nrCreatedConnections = pool.NrConnections;
            var getConnectionTasks = new List<Task<IConnection>>();

            for (var i = 0; i < 100; i++)
            {
                getConnectionTasks.Add(Task.Run(() => pool.GetAvailableConnection()));
            }
            await Task.WhenAll(getConnectionTasks);

            await Task.Delay(TimeSpan.FromMilliseconds(100));
            Assert.Equal(nrCreatedConnections, pool.NrConnections);
        }

        [Fact]
        public async Task ShouldReplaceConnectionClosedDuringSubmit()
        {
            var mockedConnectionFactory = new Mock<IConnectionFactory>();
            var fakedConnection = new Mock<IConnection>();
            fakedConnection.Setup(f => f.IsOpen).Returns(true);
            mockedConnectionFactory.Setup(m => m.CreateConnection()).Returns(fakedConnection.Object);
            var pool = CreateConnectionPool(mockedConnectionFactory.Object, 1);
            var returnedConnection = pool.GetAvailableConnection();
            fakedConnection.Setup(f => f.IsOpen).Returns(false);
            mockedConnectionFactory.Setup(m => m.CreateConnection()).Returns(OpenConnection);

            await returnedConnection.SubmitAsync<bool>(null);
            returnedConnection.Dispose();

            Assert.Equal(1, pool.NrConnections);
            Assert.True(pool.GetAvailableConnection().IsOpen);
        }

        [Fact]
        public void ShouldWaitForHostToBecomeAvailable()
        {
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory.Object, 1);
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(OpenConnection);
            var nrCreatedConnections = pool.NrConnections;
            
            var connection = pool.GetAvailableConnection();

            AssertNrOpenConnections(pool, nrCreatedConnections);
            Assert.True(connection.IsOpen);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        public void ShouldPerformConfiguredNrReconnectionAttemptsForUnavailableServer(int nrAttempts)
        {
            var mockedConnectionFactory = new Mock<IConnectionFactory>();
            mockedConnectionFactory.Setup(m => m.CreateConnection()).Returns(ClosedConnection);
            var pool = CreateConnectionPool(mockedConnectionFactory.Object, 1, nrAttempts);

            Assert.ThrowsAny<Exception>(() => pool.GetAvailableConnection());

            mockedConnectionFactory.Verify(m => m.CreateConnection(), Times.Exactly(nrAttempts + 2));
            // 2 additional calls are expected: 1 for the initial creation of the pool and 1 when the connection should
            // be returned and none is open for the first attempt (before any retries)
        }

        [Fact]
        public void ShouldThrowAfterWaitingTooLongForUnavailableServer()
        {
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory.Object, 1);
            
            Assert.Throws<ServerUnavailableException>(() => pool.GetAvailableConnection());
        }

        private static IConnection OpenConnection
        {
            get
            {
                var fakedConnection = new Mock<IConnection>();
                fakedConnection.Setup(f => f.IsOpen).Returns(true);
                return fakedConnection.Object;
            }
        }
        
        private static IConnection ClosedConnection
        {
            get
            {
                var fakedConnection = new Mock<IConnection>();
                fakedConnection.Setup(f => f.IsOpen).Returns(false);
                return fakedConnection.Object;
            }
        }

        private static ConnectionPool CreateConnectionPool(IConnectionFactory connectionFactory, int poolSize = 2,
            int reconnectionAttempts = 1)
        {
            return new ConnectionPool(connectionFactory,
                new ConnectionPoolSettings
                {
                    PoolSize = poolSize, ReconnectionAttempts = reconnectionAttempts,
                    ReconnectionBaseDelay = TimeSpan.FromMilliseconds(10)   // let the tests execute fast
                });
        }
    }
}