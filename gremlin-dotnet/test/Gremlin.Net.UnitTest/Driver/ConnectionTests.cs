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
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class ConnectionTests
    {
        [Fact]
        public async Task ShouldHandleCloseMessageAfterConnectAsync()
        {
            var mockedClientWebSocket = Substitute.For<IClientWebSocket>();
            mockedClientWebSocket.ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>()).Returns(
                new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, WebSocketCloseStatus.MessageTooBig,
                    "Message is too large"));
            mockedClientWebSocket.Options.Returns(new ClientWebSocket().Options);
            var uri = new Uri("wss://localhost:8182");
            var connection = GetConnection(mockedClientWebSocket, uri: uri);

            await connection.ConnectAsync(CancellationToken.None);

            Assert.False(connection.IsOpen);
            Assert.Equal(0, connection.NrRequestsInFlight);
            await mockedClientWebSocket.Received(1).ConnectAsync(uri, Arg.Any<CancellationToken>());
            await mockedClientWebSocket.Received(1)
                .ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>());
            await mockedClientWebSocket.Received(1).CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty,
                Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task ShouldThrowIfClosedMessageReceivedWithValidPropertiesAsync()
        {
            var mockedClientWebSocket = Substitute.For<IClientWebSocket>();
            mockedClientWebSocket.Options.Returns(new ClientWebSocket().Options);

            var webSocketConnection = new WebSocketConnection(
                mockedClientWebSocket,
                new WebSocketSettings());

            // Test all known close statuses
            foreach (Enum closeStatus in Enum.GetValues(typeof(WebSocketCloseStatus)))
            {
                mockedClientWebSocket
                    .ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>())
                    .Returns(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, (WebSocketCloseStatus)closeStatus, closeStatus.ToString()));

                await AssertExpectedConnectionClosedException((WebSocketCloseStatus?)closeStatus, closeStatus.ToString(), () => webSocketConnection.ReceiveMessageAsync());
            }

            // Test null/empty close property values as well.
            mockedClientWebSocket
                .ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>())
                .Returns(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, null, null));
            await AssertExpectedConnectionClosedException(null, null, () => webSocketConnection.ReceiveMessageAsync());

            mockedClientWebSocket
                .ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>())
                .Returns(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, null, string.Empty));
            await AssertExpectedConnectionClosedException(null, string.Empty, () => webSocketConnection.ReceiveMessageAsync());
        }

        [Fact]
        public async Task ShouldThrowOnSubmitRequestIfWebSocketIsNotOpen()
        {
            // This is to test that race bugs don't get introduced which would cause submitted requests to hang if the
            // connection is being closed/aborted or is not connected. In these cases, WebSocket.SendAsync should throw for the underlying
            // websocket is not open and the caller should be notified of that failure.
            var mockedClientWebSocket = Substitute.For<IClientWebSocket>();

            mockedClientWebSocket.Options.Returns(new ClientWebSocket().Options);

            var connection = GetConnection(mockedClientWebSocket);
            var request = RequestMessage.Build("gremlin").Create();

            // Simulate the SendAsync exception behavior if the underlying websocket is closed (see reference https://docs.microsoft.com/en-us/dotnet/api/system.net.websockets.clientwebsocket.sendasync?view=net-6.0)
            mockedClientWebSocket
                .SendAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<WebSocketMessageType>(), Arg.Any<bool>(),
                    Arg.Any<CancellationToken>())
                .ThrowsAsync(new ObjectDisposedException(nameof(ClientWebSocket), "Socket closed"));

            // Test various closing/closed WebSocketStates with SubmitAsync.
            mockedClientWebSocket.State.Returns(WebSocketState.Closed);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request, CancellationToken.None));

            mockedClientWebSocket.State.Returns(WebSocketState.CloseSent);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request, CancellationToken.None));

            mockedClientWebSocket.State.Returns(WebSocketState.CloseReceived);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request, CancellationToken.None));

            mockedClientWebSocket.State.Returns(WebSocketState.Aborted);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => connection.SubmitAsync<dynamic>(request, CancellationToken.None));

            // Simulate SendAsync exception behavior if underlying websocket is not connected.
            mockedClientWebSocket
                .SendAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<WebSocketMessageType>(), Arg.Any<bool>(),
                    Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("Socket not connected"));
            mockedClientWebSocket.State.Returns(WebSocketState.Connecting);
            await Assert.ThrowsAsync<InvalidOperationException>(() => connection.SubmitAsync<dynamic>(request, CancellationToken.None));
        }

        [Fact]
        public async Task ShouldHandleCloseMessageForInFlightRequestsAsync()
        {
            // Tests that in-flight requests will get notified if a connection close message is received.
            var uri = new Uri("wss://localhost:8182");
            var closeResult = new WebSocketReceiveResult(0, WebSocketMessageType.Close, true, WebSocketCloseStatus.EndpointUnavailable, "Server shutdown");

            var receiveSemaphore = new SemaphoreSlim(0, 1);
            var mockedClientWebSocket = Substitute.For<IClientWebSocket>();
            mockedClientWebSocket.ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>())
                .ReturnsForAnyArgs(async x =>
                {
                    await receiveSemaphore.WaitAsync();
                    mockedClientWebSocket.State.Returns(WebSocketState.CloseReceived);
                    return closeResult;
                });
            mockedClientWebSocket.State.Returns(WebSocketState.Open);
            mockedClientWebSocket.Options.Returns(new ClientWebSocket().Options);

            var connection = GetConnection(mockedClientWebSocket, uri: uri);
            await connection.ConnectAsync(CancellationToken.None);

            // Create two in-flight requests that will block on waiting for a response.
            var requestMsg1 = RequestMessage.Build("gremlin").Create();
            var requestMsg2 = RequestMessage.Build("gremlin").Create();
            Task request1 = connection.SubmitAsync<dynamic>(requestMsg1, CancellationToken.None);
            Task request2 = connection.SubmitAsync<dynamic>(requestMsg2, CancellationToken.None);

            // Confirm the requests are in-flight.
            Assert.Equal(2, connection.NrRequestsInFlight);

            // Release the connection close message.
            receiveSemaphore.Release();

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
            await mockedClientWebSocket.Received(1).ConnectAsync(uri, Arg.Any<CancellationToken>());
            await mockedClientWebSocket.Received(1)
                .ReceiveAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<CancellationToken>());
            await mockedClientWebSocket.Received(1).CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty,
                Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task ShouldProperlyHandleCancellationForSubmitAsync()
        {
            var mockedClientWebSocket = Substitute.For<IClientWebSocket>();
            mockedClientWebSocket.Options.Returns(new ClientWebSocket().Options);
            var connection = GetConnection(mockedClientWebSocket);
            var cts = new CancellationTokenSource();

            var task = connection.SubmitAsync<object>(RequestMessage.Build(string.Empty).Create(),
                cts.Token);
            cts.Cancel();

            await Assert.ThrowsAsync<TaskCanceledException>(async () => await task);
            Assert.True(task.IsCanceled);
            await mockedClientWebSocket.Received(1).SendAsync(Arg.Any<ArraySegment<byte>>(),
                Arg.Any<WebSocketMessageType>(), Arg.Any<bool>(), cts.Token);
            await mockedClientWebSocket.DidNotReceive().CloseAsync(Arg.Any<WebSocketCloseStatus>(), Arg.Any<string>(),
                Arg.Any<CancellationToken>());
            await mockedClientWebSocket.DidNotReceive().ReceiveAsync(Arg.Any<ArraySegment<byte>>(), cts.Token);
        }

        [Fact]
        public async Task ShouldProperlyHandleCancellationForSubmitAsyncIfAlreadyCancelled()
        {
            var mockedClientWebSocket = Substitute.For<IClientWebSocket>();
            mockedClientWebSocket.Options.Returns(new ClientWebSocket().Options);
            var connection = GetConnection(mockedClientWebSocket);
            var token = new CancellationToken(canceled: true);

            var task = connection.SubmitAsync<object>(RequestMessage.Build(string.Empty).Create(),
                token);

            await Assert.ThrowsAsync<TaskCanceledException>(async () => await task);
            Assert.True(task.IsCanceled);
            await mockedClientWebSocket.DidNotReceive().SendAsync(Arg.Any<ArraySegment<byte>>(),
                Arg.Any<WebSocketMessageType>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
            await mockedClientWebSocket.DidNotReceive().CloseAsync(Arg.Any<WebSocketCloseStatus>(), Arg.Any<string>(),
                Arg.Any<CancellationToken>());
            await mockedClientWebSocket.DidNotReceive().ReceiveAsync(Arg.Any<ArraySegment<byte>>(), token);
        }

        [Fact]
        public async Task ShouldContinueSubmittingOtherMessagesIfOneIsCancelled()
        {
            var mockedClientWebSocket = Substitute.For<IClientWebSocket>();
            mockedClientWebSocket.Options.Returns(new ClientWebSocket().Options);
            var tcs = new TaskCompletionSource();
            mockedClientWebSocket.SendAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<WebSocketMessageType>(),
                Arg.Any<bool>(), Arg.Any<CancellationToken>()).Returns(tcs.Task);
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
            await mockedClientWebSocket.Received(3).SendAsync(Arg.Any<ArraySegment<byte>>(),
                Arg.Any<WebSocketMessageType>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task ShouldContinueSubmittingOtherMessagesIfOneIsAlreadyCancelled()
        {
            var mockedClientWebSocket = Substitute.For<IClientWebSocket>();
            mockedClientWebSocket.Options.Returns(new ClientWebSocket().Options);
            var cancelledToken = new CancellationToken(canceled: true);
            mockedClientWebSocket.SendAsync(Arg.Any<ArraySegment<byte>>(), Arg.Any<WebSocketMessageType>(),
                    Arg.Any<bool>(), cancelledToken)
                .Throws(new TaskCanceledException(null, null, cancelledToken));
            var messageToSend = RequestMessage.Build(string.Empty).Create();
            var fakeMessageSerializer = Substitute.For<IMessageSerializer>();
            var bytesToSend = new byte[] { 1, 2, 3 };
            fakeMessageSerializer.SerializeMessageAsync(messageToSend, Arg.Any<CancellationToken>())
                .Returns(bytesToSend);
            var connection = GetConnection(mockedClientWebSocket, fakeMessageSerializer);

            var taskToCancel = connection.SubmitAsync<object>(RequestMessage.Build(string.Empty).Create(),
                cancelledToken);
            var taskToComplete = connection.SubmitAsync<object>(messageToSend, CancellationToken.None);

            await Assert.ThrowsAsync<TaskCanceledException>(async () => await taskToCancel);
            Assert.True(taskToCancel.IsCanceled);
            await mockedClientWebSocket.Received(1).SendAsync(bytesToSend, Arg.Any<WebSocketMessageType>(),
                Arg.Any<bool>(), Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task ShouldNotProcessReceivedMessageForCancelledRequest()
        {
            var fakeMessageSerializer = Substitute.For<IMessageSerializer>();
            var receivedBytes = new byte[] { 1, 2, 3 };
            var messageToCancel = RequestMessage.Build(string.Empty).Create();
            var receivedMessage = new ResponseMessage<List<object>>(messageToCancel.RequestId,
                new ResponseStatus(ResponseStatusCode.Success), new ResponseResult<List<object>>(null));
            fakeMessageSerializer.DeserializeMessageAsync(receivedBytes, Arg.Any<CancellationToken>())
                .Returns(receivedMessage);
            var fakeWebSocketConnection = Substitute.For<IWebSocketConnection>();
            var receiveTaskCompletionSource = new TaskCompletionSource<byte[]>();
            fakeWebSocketConnection.ReceiveMessageAsync().Returns(receiveTaskCompletionSource.Task);
            var connection = GetConnection(fakeWebSocketConnection, fakeMessageSerializer);
            await connection.ConnectAsync(CancellationToken.None);
            var cts = new CancellationTokenSource();

            var submitTask = connection.SubmitAsync<object>(messageToCancel, cts.Token);
            cts.Cancel();
            receiveTaskCompletionSource.SetResult(receivedBytes);
            await Assert.ThrowsAsync<TaskCanceledException>(() => submitTask);

            Assert.Equal(0, connection.NrRequestsInFlight);
        }

        private static Connection GetConnection(IClientWebSocket clientWebSocket,
            IMessageSerializer? messageSerializer = null, Uri? uri = null)
        {
            return GetConnection(new WebSocketConnection(clientWebSocket, new WebSocketSettings()), messageSerializer,
                uri);
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
