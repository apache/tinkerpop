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
            mockedConnection.Verify(m => m.ConnectAsync(It.IsAny<CancellationToken>()), Times.Exactly(poolSize));
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
        public void GetAvailableConnectionShouldEmptyPoolIfServerUnavailable()
        {
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            fakeConnectionFactory.SetupSequence(m => m.CreateConnection()).Returns(ClosedConnection)
                .Returns(ClosedConnection).Returns(ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory.Object, 3);
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(CannotConnectConnection);

            Assert.Throws<ServerUnavailableException>(() => pool.GetAvailableConnection());
                
            Assert.Equal(0, pool.NrConnections);
        }
        
        [Fact]
        public void GetAvailableConnectionShouldEventuallyRefillPoolIfEmpty()
        {
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            fakeConnectionFactory.SetupSequence(m => m.CreateConnection()).Returns(ClosedConnection)
                .Returns(ClosedConnection).Returns(ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory.Object, 3);
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(CannotConnectConnection);
            Assert.Throws<ServerUnavailableException>(() => pool.GetAvailableConnection());
            // Pool is now empty
            Assert.Equal(0, pool.NrConnections); 
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(OpenConnection);
            
            pool.GetAvailableConnection();
            
            AssertNrOpenConnections(pool, 3);
        }

        [Fact]
        public void GetAvailableConnectionsShouldEventuallyFillUpPoolIfNotFull()
        {
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            fakeConnectionFactory.SetupSequence(m => m.CreateConnection())
                .Returns(ClosedConnection)
                .Returns(ClosedConnection)
                .Returns(OpenConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory.Object, 3);
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(CannotConnectConnection);
            pool.GetAvailableConnection();
            pool.GetAvailableConnection();
            // Pool is now just partially filled
            Assert.Equal(1, pool.NrConnections); 
            
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(OpenConnection);
            pool.GetAvailableConnection();
            
            AssertNrOpenConnections(pool, 3);
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

        [Fact]
        public async Task ShouldNotLeakConnectionsIfDisposeIsCalledWhilePoolIsPopulating()
        {
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory.Object, 1);
            var mockedConnectionToBeDisposed = new Mock<IConnection>();
            var poolWasDisposedSignal = new SemaphoreSlim(0, 1);
            mockedConnectionToBeDisposed.Setup(m => m.ConnectAsync(It.IsAny<CancellationToken>()))
                .Returns((CancellationToken _) => poolWasDisposedSignal.WaitAsync(CancellationToken.None));
            var connectionWasSuccessfullyDisposed = new SemaphoreSlim(0, 1);
            mockedConnectionToBeDisposed.Setup(m => m.Dispose())
                .Callback(() => connectionWasSuccessfullyDisposed.Release());
            // We don't use the `CancellationToken` here as the connection should also be disposed if it did not
            //  react on the cancellation. This can happen if the task is cancelled just before `ConnectAsync` returns.
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(mockedConnectionToBeDisposed.Object);
            try
            {
                pool.GetAvailableConnection();
            }
            catch (ServerUnavailableException)
            {
                // expected as the pool only contains a closed connection at this point
            }
            
            pool.Dispose();
            poolWasDisposedSignal.Release();
            
            await connectionWasSuccessfullyDisposed.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(0, pool.NrConnections);
            mockedConnectionToBeDisposed.Verify(m => m.ConnectAsync(It.IsAny<CancellationToken>()), Times.Once);
            mockedConnectionToBeDisposed.Verify(m => m.Dispose(), Times.Once);
        }

        [Fact]
        public async Task DisposeShouldCancelConnectionEstablishment()
        {
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory.Object, 1, 0);
            var mockedConnectionToBeDisposed = new Mock<IConnection>();
            mockedConnectionToBeDisposed.Setup(f => f.ConnectAsync(It.IsAny<CancellationToken>()))
                .Returns((CancellationToken ct) => Task.Delay(-1, ct));
            var connectionWasSuccessfullyDisposed = new SemaphoreSlim(0, 1);
            mockedConnectionToBeDisposed.Setup(m => m.Dispose())
                .Callback(() => connectionWasSuccessfullyDisposed.Release());
            fakeConnectionFactory.Setup(m => m.CreateConnection()).Returns(mockedConnectionToBeDisposed.Object);
            try
            {
                pool.GetAvailableConnection();
            }
            catch (ServerUnavailableException)
            {
                // expected as the pool only contains a closed connection at this point
            }
            
            pool.Dispose();

            await connectionWasSuccessfullyDisposed.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(0, pool.NrConnections);
            mockedConnectionToBeDisposed.Verify(m => m.ConnectAsync(It.IsAny<CancellationToken>()));
            mockedConnectionToBeDisposed.Verify(m => m.Dispose(), Times.Once);
        }
        
        [Fact]
        public async Task ConnectionsEstablishedInParallelShouldAllBeDisposedIfOneThrowsDuringCreation()
        {
            // This test unfortunately needs a lot of knowledge about the inner working of the ConnectionPool to 
            //  adequately test that connections established in parallel will all be disposed if one throws an
            //  exception.
            
            // First create a pool with only closed connections that we can then let the pool replace:
            var fakeConnectionFactory = new Mock<IConnectionFactory>();
            fakeConnectionFactory.SetupSequence(m => m.CreateConnection())
                .Returns(ClosedConnection) // We need to do it like this as we use a dictionary of dead connections in 
                .Returns(ClosedConnection) //   ConnectionPool and the three connections need to be different objects
                .Returns(ClosedConnection);//   for this to work.
            var pool = CreateConnectionPool(fakeConnectionFactory.Object, 3, 0);
            var startEstablishingProblematicConnections = new SemaphoreSlim(0, 1);
            // Let the pool get one connection that is so slow to open that the pool will afterwards try to create two
            //  more connections in parallel.
            var fakedSlowToEstablishConnection = new Mock<IConnection>();
            fakedSlowToEstablishConnection.Setup(m => m.ConnectAsync(It.IsAny<CancellationToken>()))
                .Returns(startEstablishingProblematicConnections.WaitAsync);
            fakeConnectionFactory.Setup(m => m.CreateConnection())
                .Returns(fakedSlowToEstablishConnection.Object);
            // Trigger replacement of closed connections
            try
            {
                pool.GetAvailableConnection();
            }
            catch (ServerUnavailableException)
            {
                // expected as the pool only contain closed connections at this point
            }
            
            var fakedOpenConnection = FakedOpenConnection;
            var fakedCannotConnectConnection = FakedCannotConnectConnection;
            fakeConnectionFactory.SetupSequence(m => m.CreateConnection())
                .Returns(fakedOpenConnection.Object)
                .Returns(fakedCannotConnectConnection.Object);
            // Let the slow to establish connection finish so the pool can try to establish the other two connections
            startEstablishingProblematicConnections.Release();
            await Task.Delay(TimeSpan.FromMilliseconds(200));
            
            // Verify that the pool tried to establish both connections and then also disposed both, even though one throw an exception
            fakedOpenConnection.Verify(m => m.ConnectAsync(It.IsAny<CancellationToken>()), Times.Once());
            fakedOpenConnection.Verify(m => m.Dispose(), Times.Once);
            fakedCannotConnectConnection.Verify(m => m.ConnectAsync(It.IsAny<CancellationToken>()), Times.Once);
            fakedCannotConnectConnection.Verify(m => m.Dispose(), Times.Once);
        }

        private static IConnection OpenConnection => FakedOpenConnection.Object;

        private static Mock<IConnection> FakedOpenConnection
        {
            get
            {
                var fakedConnection = new Mock<IConnection>();
                fakedConnection.Setup(f => f.IsOpen).Returns(true);
                return fakedConnection;
            }
        }

        private static IConnection ClosedConnection => FakedClosedConnection.Object;
        
        private static Mock<IConnection> FakedClosedConnection
        {
            get
            {
                var fakedConnection = new Mock<IConnection>();
                fakedConnection.Setup(f => f.IsOpen).Returns(false);
                return fakedConnection;
            }
        }

        private static IConnection CannotConnectConnection => FakedCannotConnectConnection.Object;
        
        private static Mock<IConnection> FakedCannotConnectConnection
        {
            get
            {
                var fakedConnection = new Mock<IConnection>();
                fakedConnection.Setup(f => f.IsOpen).Returns(false);
                fakedConnection.Setup(f => f.ConnectAsync(It.IsAny<CancellationToken>()))
                    .Throws(new Exception("Cannot connect to server."));
                return fakedConnection;
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