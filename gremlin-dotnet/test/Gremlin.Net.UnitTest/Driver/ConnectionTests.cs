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
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO.GraphBinary;
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
            Connection connection = GetConnection(mockedClientWebSocket, uri: uri);

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
            var mockedClientWebSocket = new Mock<IClientWebSocket>();

            mockedClientWebSocket
                .SetupGet(m => m.Options).Returns(new ClientWebSocket().Options);

            Connection connection = GetConnection(mockedClientWebSocket);
            RequestMessage request = RequestMessage.Build("gremlin").Create();

            // Simulate the SendAsync exception behavior if the underlying websocket is closed (see reference https://docs.microsoft.com/en-us/dotnet/api/system.net.websockets.clientwebsocket.sendasync?view=net-6.0)
            mockedClientWebSocket.Setup(m => m.SendAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<WebSocketMessageType>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ObjectDisposedException(nameof(ClientWebSocket), "Socket closed"));

            // Test various closing/closed WebSocketStates with SubmitAsync.
            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.Closed);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request, CancellationToken.None));

            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.CloseSent);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request, CancellationToken.None));

            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.CloseReceived);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request, CancellationToken.None));

            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.Aborted);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request, CancellationToken.None));

            // Simulate SendAsync exception behavior if underlying websocket is not connected.
            mockedClientWebSocket.Setup(m => m.SendAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<WebSocketMessageType>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("Socket not connected"));
            mockedClientWebSocket.Setup(m => m.State).Returns(WebSocketState.Connecting);
            await Assert.ThrowsAsync<InvalidOperationException>(() => connection.SubmitAsync<dynamic>(request, CancellationToken.None));
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

            var connection = GetConnection(mockedClientWebSocket, uri: uri);
            await connection.ConnectAsync(CancellationToken.None);

            // Create two in-flight requests that will block on waiting for a response.
            RequestMessage requestMsg1 = RequestMessage.Build("gremlin").Create();
            RequestMessage requestMsg2 = RequestMessage.Build("gremlin").Create();
            Task request1 = connection.SubmitAsync<dynamic>(requestMsg1, CancellationToken.None);
            Task request2 = connection.SubmitAsync<dynamic>(requestMsg2, CancellationToken.None);

            // Confirm the requests are in-flight.
            Assert.Equal(2, connection.NrRequestsInFlight);

            // Release the connection close message.
            receiveSempahore.Release();

            // Assert that both requests get notified with the closed exception.
            await AssertExpectedConnectionClosedException(closeResult.CloseStatus, closeResult.CloseStatusDescription, () => request1);
            await AssertExpectedConnectionClosedException(closeResult.CloseStatus, closeResult.CloseStatusDescription, () => request2);

            // delay for NotifyAboutConnectionFailure running in another thread
            var runs = 0;
            while (connection.NrRequestsInFlight == 2 && ++runs < 5)
            {
                await Task.Delay(5);
            }

            Assert.False(connection.IsOpen);
            Assert.Equal(0, connection.NrRequestsInFlight);
            mockedClientWebSocket.Verify(m => m.ConnectAsync(uri, It.IsAny<CancellationToken>()), Times.Once);
            mockedClientWebSocket.Verify(m => m.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()), Times.Once);
            mockedClientWebSocket.Verify(m => m.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ShouldProperlyHandleCancellationForSubmitAsync()
        {
            var mockedClientWebSocket = new Mock<IClientWebSocket>();
            mockedClientWebSocket
                .SetupGet(m => m.Options).Returns(new ClientWebSocket().Options);
            var connection = GetConnection(mockedClientWebSocket);
            var cts = new CancellationTokenSource();

            var task = connection.SubmitAsync<object>(RequestMessage.Build(string.Empty).Create(),
                cts.Token);
            cts.Cancel();
            
            await Assert.ThrowsAsync<TaskCanceledException>(async () => await task);
            Assert.True(task.IsCanceled);
            mockedClientWebSocket.Verify(m => m.SendAsync(It.IsAny<ArraySegment<byte>>(),
                It.IsAny<WebSocketMessageType>(), It.IsAny<bool>(), cts.Token), Times.Once);
            mockedClientWebSocket.Verify(
                m => m.CloseAsync(It.IsAny<WebSocketCloseStatus>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
            mockedClientWebSocket.Verify(
                m => m.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), cts.Token), Times.Never);
        }
        
        [Fact]
        public async Task ShouldProperlyHandleCancellationForSubmitAsyncIfAlreadyCancelled()
        {
            var mockedClientWebSocket = new Mock<IClientWebSocket>();
            mockedClientWebSocket
                .SetupGet(m => m.Options).Returns(new ClientWebSocket().Options);
            var connection = GetConnection(mockedClientWebSocket);
            var token = new CancellationToken(canceled: true);

            var task = connection.SubmitAsync<object>(RequestMessage.Build(string.Empty).Create(),
                token);
            
            await Assert.ThrowsAsync<TaskCanceledException>(async () => await task);
            Assert.True(task.IsCanceled);
            mockedClientWebSocket.Verify(
                m => m.SendAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<WebSocketMessageType>(), It.IsAny<bool>(),
                    It.IsAny<CancellationToken>()), Times.Never);
            mockedClientWebSocket.Verify(
                m => m.CloseAsync(It.IsAny<WebSocketCloseStatus>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
            mockedClientWebSocket.Verify(
                m => m.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), token), Times.Never);
        }
        
        [Fact]
        public async Task ShouldContinueSubmittingOtherMessagesIfOneIsCancelled()
        {
            var mockedClientWebSocket = new Mock<IClientWebSocket>();
            mockedClientWebSocket
                .SetupGet(m => m.Options).Returns(new ClientWebSocket().Options);
            var tcs = new TaskCompletionSource();
            mockedClientWebSocket.Setup(m => m.SendAsync(It.IsAny<ArraySegment<byte>>(),
                It.IsAny<WebSocketMessageType>(), It.IsAny<bool>(), It.IsAny<CancellationToken>())).Returns(tcs.Task);
            var connection = GetConnection(mockedClientWebSocket);
            var cts = new CancellationTokenSource();
            
            var taskToCancel = connection.SubmitAsync<object>(RequestMessage.Build(string.Empty).Create(),
                cts.Token);
            var taskToComplete = connection.SubmitAsync<object>(RequestMessage.Build(string.Empty).Create(),
                CancellationToken.None);
            cts.Cancel();
            var taskToComplete2 = connection.SubmitAsync<object>(RequestMessage.Build(string.Empty).Create(),
                CancellationToken.None);
            tcs.TrySetResult();
            
            await Assert.ThrowsAsync<TaskCanceledException>(async () => await taskToCancel);
            Assert.True(taskToCancel.IsCanceled);
            await Task.Delay(TimeSpan.FromMilliseconds(200)); // wait a bit to let the messages being sent
            mockedClientWebSocket.Verify(m => m.SendAsync(It.IsAny<ArraySegment<byte>>(),
                It.IsAny<WebSocketMessageType>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
        }
        
        [Fact]
        public async Task ShouldContinueSubmittingOtherMessagesIfOneIsAlreadyCancelled()
        {
            var mockedClientWebSocket = new Mock<IClientWebSocket>();
            mockedClientWebSocket
                .SetupGet(m => m.Options).Returns(new ClientWebSocket().Options);
            var cancelledToken = new CancellationToken(canceled: true);
            mockedClientWebSocket
                .Setup(m => m.SendAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<WebSocketMessageType>(),
                    It.IsAny<bool>(), cancelledToken))
                .ThrowsAsync(new TaskCanceledException(null, null, cancelledToken));
            var messageToSend = RequestMessage.Build(string.Empty).Create();
            var fakeMessageSerializer = new Mock<IMessageSerializer>();
            var bytesToSend = new byte[] { 1, 2, 3 };
            fakeMessageSerializer.Setup(f => f.SerializeMessageAsync(messageToSend, It.IsAny<CancellationToken>()))
                .ReturnsAsync(bytesToSend);
            var connection = GetConnection(mockedClientWebSocket, fakeMessageSerializer.Object);
            
            var taskToCancel = connection.SubmitAsync<object>(RequestMessage.Build(string.Empty).Create(),
                cancelledToken);
            var taskToComplete = connection.SubmitAsync<object>(messageToSend, CancellationToken.None);
            
            await Assert.ThrowsAsync<TaskCanceledException>(async () => await taskToCancel);
            Assert.True(taskToCancel.IsCanceled);
            mockedClientWebSocket.Verify(m => m.SendAsync(bytesToSend,
                It.IsAny<WebSocketMessageType>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
        }

        [Fact]
        public async Task ShouldNotProcessReceivedMessageForCancelledRequest()
        {
            var fakeMessageSerializer = new Mock<IMessageSerializer>();
            var receivedBytes = new byte[] { 1, 2, 3 };
            var messageToCancel = RequestMessage.Build(string.Empty).Create();
            var receivedMessage = new ResponseMessage<List<object>>(messageToCancel.RequestId,
                new ResponseStatus(ResponseStatusCode.Success), new ResponseResult<List<object>>(null));
            fakeMessageSerializer.Setup(f => f.DeserializeMessageAsync(receivedBytes, It.IsAny<CancellationToken>()))
                .ReturnsAsync(receivedMessage);
            var fakeWebSocketConnection = new Mock<IWebSocketConnection>();
            var receiveTaskCompletionSource = new TaskCompletionSource<byte[]>();
            fakeWebSocketConnection.Setup(m => m.ReceiveMessageAsync()).Returns(receiveTaskCompletionSource.Task);
            var connection = GetConnection(fakeWebSocketConnection.Object, fakeMessageSerializer.Object);
            await connection.ConnectAsync(CancellationToken.None);
            var cts = new CancellationTokenSource();
            
            var submitTask = connection.SubmitAsync<object>(messageToCancel, cts.Token);
            cts.Cancel();
            receiveTaskCompletionSource.SetResult(receivedBytes);
            await Assert.ThrowsAsync<TaskCanceledException>(() => submitTask);
            
            Assert.Equal(0, connection.NrRequestsInFlight);
        }

        private static Connection GetConnection(IMock<IClientWebSocket> mockedClientWebSocket,
            IMessageSerializer? messageSerializer = null, Uri? uri = null)
        {
            return GetConnection(new WebSocketConnection(mockedClientWebSocket.Object, new WebSocketSettings()),
                messageSerializer, uri);
        }
        
        private static Connection GetConnection(IWebSocketConnection webSocketConnection,
            IMessageSerializer? messageSerializer = null, Uri? uri = null)
        {
            uri ??= new Uri("wss://localhost:8182");
            messageSerializer ??= new GraphBinaryMessageSerializer();
            return new Connection(
                webSocketConnection,
                uri: uri,
                username: "user",
                password: "password",
                messageSerializer: messageSerializer,
                sessionId: null);
        }

        private static async Task AssertExpectedConnectionClosedException(WebSocketCloseStatus? expectedCloseStatus,
            string? expectedCloseDescription, Func<Task> func)
        {
            var exception = await Assert.ThrowsAsync<ConnectionClosedException>(func);
            Assert.Equal(expectedCloseStatus, exception.Status);
            Assert.Equal(expectedCloseDescription, exception.Description);
        }
    }
}
