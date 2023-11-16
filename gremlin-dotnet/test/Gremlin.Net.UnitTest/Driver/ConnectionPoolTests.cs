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
using Gremlin.Net.Driver.Messages;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using NSubstitute.Extensions;
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
            var mockedConnectionFactory = Substitute.For<IConnectionFactory>();
            var mockedConnection = Substitute.For<IConnection>();
            mockedConnectionFactory.CreateConnection().Returns(mockedConnection);
            var pool = CreateConnectionPool(mockedConnectionFactory, poolSize);
            
            Assert.Equal(poolSize, pool.NrConnections);
            mockedConnectionFactory.Received(poolSize).CreateConnection();
            mockedConnection.Received(poolSize).ConnectAsync(Arg.Any<CancellationToken>());
        }

        [Fact]
        public void GetAvailableConnectionShouldReturnFirstOpenConnection()
        {
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            var openConnectionToReturn = OpenConnection;
            fakeConnectionFactory.CreateConnection().Returns(_ => ClosedConnection, _ => ClosedConnection,
                _ => openConnectionToReturn);
            var pool = CreateConnectionPool(fakeConnectionFactory, 3);

            var returnedConnection = pool.GetAvailableConnection();

            Assert.Equal(openConnectionToReturn, ((ProxyConnection) returnedConnection).ProxiedConnection);
        }
        
        [Fact]
        public void GetAvailableConnectionShouldThrowIfAllConnectionsAreClosed()
        {
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            fakeConnectionFactory.CreateConnection().Returns(_ => ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory);

            Assert.Throws<ServerUnavailableException>(() => pool.GetAvailableConnection());
        }

        [Fact]
        public void GetAvailableConnectionShouldEmptyPoolIfServerUnavailable()
        {
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            fakeConnectionFactory.CreateConnection()
                .Returns(_ => ClosedConnection, _ => ClosedConnection, _ => ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory, 3);
            fakeConnectionFactory.Configure().CreateConnection().Returns(_ => CannotConnectConnection);

            Assert.Throws<ServerUnavailableException>(() => pool.GetAvailableConnection());
                
            Assert.Equal(0, pool.NrConnections);
        }
        
        [Fact]
        public void GetAvailableConnectionShouldEventuallyRefillPoolIfEmpty()
        {
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            fakeConnectionFactory.CreateConnection()
                .Returns(_ => ClosedConnection, _ => ClosedConnection, _ => ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory, 3);
            fakeConnectionFactory.Configure().CreateConnection().Returns(_ => CannotConnectConnection);
            Assert.Throws<ServerUnavailableException>(() => pool.GetAvailableConnection());
            // Pool is now empty
            Assert.Equal(0, pool.NrConnections);
            fakeConnectionFactory.Configure().CreateConnection().Returns(_ => OpenConnection);
            
            pool.GetAvailableConnection();
            
            AssertNrOpenConnections(pool, 3);
        }

        [Fact]
        public void GetAvailableConnectionsShouldEventuallyFillUpPoolIfNotFull()
        {
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            fakeConnectionFactory.CreateConnection()
                .Returns(_ => ClosedConnection, _ => ClosedConnection, _ => OpenConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory, 3);
            fakeConnectionFactory.Configure().CreateConnection().Returns(_ => CannotConnectConnection);
            pool.GetAvailableConnection();
            pool.GetAvailableConnection();
            // Pool is now just partially filled
            Assert.Equal(1, pool.NrConnections); 
            
            fakeConnectionFactory.Configure().CreateConnection().Returns(_ => OpenConnection);
            pool.GetAvailableConnection();
            
            AssertNrOpenConnections(pool, 3);
        }
        
        [Fact]
        public void GetAvailableConnectionShouldReplaceClosedConnections()
        {
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            fakeConnectionFactory.CreateConnection()
                .Returns(_ => ClosedConnection, _ => ClosedConnection, _ => OpenConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory, 3);
            fakeConnectionFactory.Configure().CreateConnection().Returns(_ => OpenConnection);
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
            var mockedConnectionFactory = Substitute.For<IConnectionFactory>();
            mockedConnectionFactory.CreateConnection()
                .Returns(_ => ClosedConnection, _ => ClosedConnection, _ => OpenConnection);
            var pool = CreateConnectionPool(mockedConnectionFactory, 3);
            mockedConnectionFactory.Configure().CreateConnection().Returns(_ => OpenConnection);
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
            var mockedConnectionFactory = Substitute.For<IConnectionFactory>();
            var fakedConnection = Substitute.For<IConnection>();
            fakedConnection.IsOpen.Returns(true);
            mockedConnectionFactory.CreateConnection().Returns(fakedConnection);
            var pool = CreateConnectionPool(mockedConnectionFactory, 1);
            var returnedConnection = pool.GetAvailableConnection();
            fakedConnection.IsOpen.Returns(false);
            mockedConnectionFactory.CreateConnection().Returns(_ => OpenConnection);

            await returnedConnection.SubmitAsync<bool>(RequestMessage.Build(string.Empty).Create(),
                CancellationToken.None);
            returnedConnection.Dispose();

            Assert.Equal(1, pool.NrConnections);
            Assert.True(pool.GetAvailableConnection().IsOpen);
        }

        [Fact]
        public void ShouldWaitForHostToBecomeAvailable()
        {
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            fakeConnectionFactory.CreateConnection().Returns(_ => ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory, 1);
            fakeConnectionFactory.Configure().CreateConnection().Returns(_ => OpenConnection);
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
            var mockedConnectionFactory = Substitute.For<IConnectionFactory>();
            mockedConnectionFactory.CreateConnection().Returns(_ => ClosedConnection);
            var pool = CreateConnectionPool(mockedConnectionFactory, 1, nrAttempts);

            Assert.ThrowsAny<Exception>(() => pool.GetAvailableConnection());

            mockedConnectionFactory.Received(nrAttempts + 2).CreateConnection();
            // 2 additional calls are expected: 1 for the initial creation of the pool and 1 when the connection should
            // be returned and none is open for the first attempt (before any retries)
        }

        [Fact]
        public void ShouldThrowAfterWaitingTooLongForUnavailableServer()
        {
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            fakeConnectionFactory.CreateConnection().Returns(_ => ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory, 1);
            
            Assert.Throws<ServerUnavailableException>(() => pool.GetAvailableConnection());
        }
        
        [Fact]
        public async Task ShouldNotLeakConnectionsIfDisposeIsCalledWhilePoolIsPopulating()
        {
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            fakeConnectionFactory.CreateConnection().Returns(_ => ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory, 1);
            var mockedConnectionToBeDisposed = Substitute.For<IConnection>();
            var poolWasDisposedSignal = new SemaphoreSlim(0, 1);
            mockedConnectionToBeDisposed.ConnectAsync(Arg.Any<CancellationToken>())
                .Returns(x => poolWasDisposedSignal.WaitAsync(CancellationToken.None));
            var connectionWasSuccessfullyDisposed = new SemaphoreSlim(0, 1);
            mockedConnectionToBeDisposed.When(x => x.Dispose())
                .Do(_ => connectionWasSuccessfullyDisposed.Release());
            // We don't use the `CancellationToken` here as the connection should also be disposed if it did not
            //  react on the cancellation. This can happen if the task is cancelled just before `ConnectAsync` returns.
            fakeConnectionFactory.Configure().CreateConnection().Returns(mockedConnectionToBeDisposed);
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
            await mockedConnectionToBeDisposed.Received(1).ConnectAsync(Arg.Any<CancellationToken>());
            mockedConnectionToBeDisposed.Received(1).Dispose();
        }
        
        [Fact]
        public async Task DisposeShouldCancelConnectionEstablishment()
        {
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            fakeConnectionFactory.CreateConnection().Returns(_ => ClosedConnection);
            var pool = CreateConnectionPool(fakeConnectionFactory, 1, 0);
            var mockedConnectionToBeDisposed = Substitute.For<IConnection>();
            mockedConnectionToBeDisposed.ConnectAsync(Arg.Any<CancellationToken>())
                .Returns(x => Task.Delay(-1, (CancellationToken)x[0]));
            var connectionWasSuccessfullyDisposed = new SemaphoreSlim(0, 1);
            mockedConnectionToBeDisposed.When(x => x.Dispose())
                .Do(_ => connectionWasSuccessfullyDisposed.Release());
            fakeConnectionFactory.Configure().CreateConnection().Returns(mockedConnectionToBeDisposed);
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
            await mockedConnectionToBeDisposed.Received(1).ConnectAsync(Arg.Any<CancellationToken>());
            mockedConnectionToBeDisposed.Received(1).Dispose();
        }
        
        [Fact]
        public async Task ConnectionsEstablishedInParallelShouldAllBeDisposedIfOneThrowsDuringCreation()
        {
            // This test unfortunately needs a lot of knowledge about the inner working of the ConnectionPool to 
            //  adequately test that connections established in parallel will all be disposed if one throws an
            //  exception.
            
            // First create a pool with only closed connections that we can then let the pool replace:
            var fakeConnectionFactory = Substitute.For<IConnectionFactory>();
            fakeConnectionFactory.CreateConnection()
                .Returns(_ => ClosedConnection,      // We need to do it like this as we use a dictionary of dead connections in 
                    _ => ClosedConnection,    //   ConnectionPool and the three connections need to be different objects
                    _ => ClosedConnection);                  //   for this to work.
            var pool = CreateConnectionPool(fakeConnectionFactory, 3, 0);
            var startEstablishingProblematicConnections = new SemaphoreSlim(0, 1);
            // Let the pool get one connection that is so slow to open that the pool will afterwards try to create two
            //  more connections in parallel.
            var fakedSlowToEstablishConnection = Substitute.For<IConnection>();
            fakedSlowToEstablishConnection.ConnectAsync(Arg.Any<CancellationToken>())
                .Returns(async _ => await startEstablishingProblematicConnections.WaitAsync());
            fakeConnectionFactory.Configure().CreateConnection().Returns(fakedSlowToEstablishConnection);
            // Trigger replacement of closed connections
            try
            {
                pool.GetAvailableConnection();
            }
            catch (ServerUnavailableException)
            {
                // expected as the pool only contain closed connections at this point
            }
            
            var fakedOpenConnection = OpenConnection;
            var fakedCannotConnectConnection = CannotConnectConnection;
            fakeConnectionFactory.Configure().CreateConnection()
                .Returns(fakedOpenConnection, fakedCannotConnectConnection);
            // Let the slow to establish connection finish so the pool can try to establish the other two connections
            startEstablishingProblematicConnections.Release();
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            
            // Verify that the pool tried to establish both connections and then also disposed both, even though one throw an exception
            await fakedOpenConnection.Received(1).ConnectAsync(Arg.Any<CancellationToken>());
            fakedOpenConnection.Received(1).Dispose();
            await fakedCannotConnectConnection.Received(1).ConnectAsync(Arg.Any<CancellationToken>());
            fakedCannotConnectConnection.Received(1).Dispose();
        }

        private static IConnection OpenConnection
        {
            get
            {
                var fakedConnection = Substitute.For<IConnection>();
                fakedConnection.IsOpen.Returns(true);
                return fakedConnection;
            }
        }
        
        private static IConnection ClosedConnection
        {
            get
            {
                var fakedConnection = Substitute.For<IConnection>();
                fakedConnection.IsOpen.Returns(false);
                return fakedConnection;
            }
        }
        
        private static IConnection CannotConnectConnection
        {
            get
            {
                var fakedConnection = Substitute.For<IConnection>();
                fakedConnection.IsOpen.Returns(false);
                fakedConnection.ConnectAsync(Arg.Any<CancellationToken>())
                    .Throws(new Exception("Cannot connect to server."));
                return fakedConnection;
            }
        }

        private static ConnectionPool CreateConnectionPool(IConnectionFactory connectionFactory, int poolSize = 2,
            int reconnectionAttempts = 1)
        {
            return new ConnectionPool(connectionFactory, new ConnectionPoolSettings
            {
                PoolSize = poolSize, ReconnectionAttempts = reconnectionAttempts,
                ReconnectionBaseDelay = TimeSpan.FromMilliseconds(10) // let the tests execute fast
            }, NullLogger<ConnectionPool>.Instance);
        }
    }
}