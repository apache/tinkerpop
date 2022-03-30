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
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO.GraphSON;
using Moq;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class ConnectionTests
    {
        [Fact]
        public async Task ShouldHandleCloseMessageAfterConnectAsync()
        {
            var mockedClientWebSocket = new Mock<IClientWebSocket>();
            mockedClientWebSocket
                .Setup(m => m.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, WebSocketCloseStatus.MessageTooBig, "Message is too large"));
            mockedClientWebSocket
                .SetupGet(m => m.Options).Returns(new ClientWebSocket().Options);

            Uri uri = new Uri("wss://localhost:8182");
            Connection connection = GetConnection(mockedClientWebSocket, uri);

            await connection.ConnectAsync(CancellationToken.None);

            Assert.False(connection.IsOpen);
            Assert.Equal(0, connection.NrRequestsInFlight);
            mockedClientWebSocket.Verify(m => m.ConnectAsync(uri, It.IsAny<CancellationToken>()), Times.Once);
            mockedClientWebSocket.Verify(m => m.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()), Times.Once);
            mockedClientWebSocket.Verify(m => m.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ShouldThrowIfClosedMessageReceivedWithValidPropertiesAsync()
        {
            var mockedClientWebSocket = new Mock<IClientWebSocket>();
            mockedClientWebSocket
                .SetupGet(m => m.Options).Returns(new ClientWebSocket().Options);

            WebSocketConnection webSocketConnection = new WebSocketConnection(
                mockedClientWebSocket.Object,
                new WebSocketSettings());

            // Test all known close statuses
            foreach (Enum closeStatus in Enum.GetValues(typeof(WebSocketCloseStatus)))
            {
                mockedClientWebSocket
                    .Setup(m => m.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, (WebSocketCloseStatus)closeStatus, closeStatus.ToString()));
    
                await AssertExpectedConnectionClosedException((WebSocketCloseStatus?)closeStatus, closeStatus.ToString(), () => webSocketConnection.ReceiveMessageAsync());
            }

            // Test null/empty close property values as well.
            mockedClientWebSocket
                .Setup(m => m.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, null, null));
            await AssertExpectedConnectionClosedException(null, null, () => webSocketConnection.ReceiveMessageAsync());
            
            mockedClientWebSocket
                .Setup(m => m.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, null, String.Empty));
            await AssertExpectedConnectionClosedException(null, String.Empty, () => webSocketConnection.ReceiveMessageAsync());
        }

        [Fact]
        public async Task ShouldThrowOnSubmitRequestIfWebSocketIsNotOpen()
        {
            // This is to test that race bugs don't get introduced which would cause submitted requests to hang if the
            // connection is being closed/aborted or is not connected. In these cases, WebSocket.SendAsync should throw for the underlying
            // websocket is not open and the caller should be notified of that failure.
            Uri uri = new Uri("wss://localhost:8182");
            var mockedClientWebSocket = new Mock<IClientWebSocket>();

            mockedClientWebSocket
                .SetupGet(m => m.Options).Returns(new ClientWebSocket().Options);

            Connection connection = GetConnection(mockedClientWebSocket, uri);
            RequestMessage request = RequestMessage.Build("gremlin").Create();

            // Simulate the SendAsync exception behavior if the underlying websocket is closed (see reference https://docs.microsoft.com/en-us/dotnet/api/system.net.websockets.clientwebsocket.sendasync?view=net-6.0)
            mockedClientWebSocket.Setup(m => m.SendAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<WebSocketMessageType>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ObjectDisposedException(nameof(ClientWebSocket), "Socket closed"));

            // Test various closing/closed WebSocketStates with SubmitAsync.
            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.Closed);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request));

            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.CloseSent);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request));

            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.CloseReceived);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request));

            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.Aborted);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request));

            // Simulate SendAsync exception behavior if underlying websocket is not connected.
            mockedClientWebSocket.Setup(m => m.SendAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<WebSocketMessageType>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("Socket not connected"));
            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.Connecting);
            await Assert.ThrowsAsync<InvalidOperationException>(() => connection.SubmitAsync<dynamic>(request));
        }

        [Fact]
        public async Task ShouldHandleCloseMessageForInFlightRequestsAsync()
        {
            // Tests that in-flight requests will get notified if a connection close message is received.
            Uri uri = new Uri("wss://localhost:8182");
            WebSocketReceiveResult closeResult = new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, WebSocketCloseStatus.EndpointUnavailable, "Server shutdown");

            var receiveSempahore = new SemaphoreSlim(0, 1);
            var mockedClientWebSocket = new Mock<IClientWebSocket>();
            mockedClientWebSocket
                .Setup(m => m.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    await receiveSempahore.WaitAsync();
                    mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.CloseReceived);
                    return closeResult;
                });
            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.Open);
            mockedClientWebSocket
                .SetupGet(m => m.Options).Returns(new ClientWebSocket().Options);

            Connection connection = GetConnection(mockedClientWebSocket, uri);
            await connection.ConnectAsync(CancellationToken.None);

            // Create two in-flight requests that will block on waiting for a response.
            RequestMessage requestMsg1 = RequestMessage.Build("gremlin").Create();
            RequestMessage requestMsg2 = RequestMessage.Build("gremlin").Create();
            Task request1 = connection.SubmitAsync<dynamic>(requestMsg1);
            Task request2 = connection.SubmitAsync<dynamic>(requestMsg2);

            // Confirm the requests are in-flight.
            Assert.Equal(2, connection.NrRequestsInFlight);

            // Release the connection close message.
            receiveSempahore.Release();

            // Assert that both requests get notified with the closed exception.
            await AssertExpectedConnectionClosedException(closeResult.CloseStatus, closeResult.CloseStatusDescription, () => request1);
            await AssertExpectedConnectionClosedException(closeResult.CloseStatus, closeResult.CloseStatusDescription, () => request2);

            Assert.False(connection.IsOpen);
            Assert.Equal(0, connection.NrRequestsInFlight);
            mockedClientWebSocket.Verify(m => m.ConnectAsync(uri, It.IsAny<CancellationToken>()), Times.Once);
            mockedClientWebSocket.Verify(m => m.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()), Times.Once);
            mockedClientWebSocket.Verify(m => m.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, It.IsAny<CancellationToken>()), Times.Once);
        }


        private static Connection GetConnection(Mock<IClientWebSocket> mockedClientWebSocket, Uri uri)
        {
            return new Connection(
                mockedClientWebSocket.Object,
                uri: uri,
                username: "user",
                password: "password",
                messageSerializer: new GraphSON3MessageSerializer(),
                webSocketSettings: new WebSocketSettings(),
                sessionId: null);
        }

        private static async Task AssertExpectedConnectionClosedException(WebSocketCloseStatus? expectedCloseStatus, string expectedCloseDescription, Func<Task> func)
        {
            ConnectionClosedException exception = await Assert.ThrowsAsync<ConnectionClosedException>(func);
            Assert.Equal(expectedCloseStatus, exception.Status);
            Assert.Equal(expectedCloseDescription, exception.Description);
        }
    }
}
