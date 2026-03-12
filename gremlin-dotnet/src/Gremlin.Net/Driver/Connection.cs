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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process;

namespace Gremlin.Net.Driver
{
    internal interface IResponseHandlerForSingleRequestMessage
    {
        void HandleReceived(ResponseMessage<List<object>> received);
        void Finalize(Dictionary<string, object> statusAttributes);
        void HandleFailure(Exception objException);
        void Cancel();
    }

    internal class Connection : IConnection
    {
        private readonly IMessageSerializer _messageSerializer;
        private readonly Uri _uri;
        private readonly IWebSocketConnection _webSocketConnection;
        private readonly string? _username;
        private readonly string? _password;
        private readonly string? _sessionId;

        private readonly ConcurrentQueue<(RequestMessage msg, CancellationToken cancellationToken)> _writeQueue = new();

        private readonly ConcurrentDictionary<Guid, IResponseHandlerForSingleRequestMessage> _callbackByRequestId =
            new();

        private readonly ConcurrentDictionary<Guid, CancellationTokenRegistration> _cancellationByRequestId = new();
        private int _connectionState = 0;
        private int _writeInProgress = 0;
        private const int Closed = 1;

        public Connection(IWebSocketConnection webSocketConnection, Uri uri, string? username, string? password,
            IMessageSerializer messageSerializer, string? sessionId)
        {
            _uri = uri;
            _username = username;
            _password = password;
            _sessionId = sessionId;
            _messageSerializer = messageSerializer;
            _webSocketConnection = webSocketConnection;
        }

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            await _webSocketConnection.ConnectAsync(_uri, cancellationToken).ConfigureAwait(false);
            BeginReceiving();
        }

        public int NrRequestsInFlight => _callbackByRequestId.Count;

        public bool IsOpen => _webSocketConnection.IsOpen && Volatile.Read(ref _connectionState) != Closed;

        public Task<ResultSet<T>> SubmitAsync<T>(RequestMessage requestMessage, CancellationToken cancellationToken)
        {
            var receiver = new ResponseHandlerForSingleRequestMessage<T>();
            var requestId = Guid.NewGuid();
            _callbackByRequestId.GetOrAdd(requestId, receiver);

            _cancellationByRequestId.GetOrAdd(requestId, cancellationToken.Register(() =>
            {
                if (_callbackByRequestId.TryRemove(requestId, out var responseHandler))
                {
                    responseHandler.Cancel();
                }
            }));
            _writeQueue.Enqueue((requestMessage, cancellationToken));
            BeginSendingMessages();
            return receiver.Result;
        }

        private void BeginReceiving()
        {
            var state = Volatile.Read(ref _connectionState);
            if (state == Closed) return;
            ReceiveMessagesAsync().Forget();
        }

        private async Task ReceiveMessagesAsync()
        {
            while (true)
            {
                try
                {
                    var received = await _webSocketConnection.ReceiveMessageAsync().ConfigureAwait(false);
                    await HandleReceivedAsync(received).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    await CloseConnectionBecauseOfFailureAsync(e).ConfigureAwait(false);
                    break;
                }
            }
        }

        private async Task HandleReceivedAsync(byte[] received)
        {
            var receivedMsg = await _messageSerializer.DeserializeMessageAsync(received).ConfigureAwait(false);
            try
            {
                HandleReceivedMessage(receivedMsg);
            }
            catch (Exception e)
            {
                // Propagate failure to all pending handlers
                foreach (var cb in _callbackByRequestId.Values)
                {
                    cb.HandleFailure(e);
                }
                _callbackByRequestId.Clear();
                DisposeCancellationRegistrations();
            }
        }

        private void HandleReceivedMessage(ResponseMessage<List<object>> receivedMsg)
        {
            if (receivedMsg.StatusCode != 200)
            {
                throw new Exceptions.ResponseException(receivedMsg.StatusCode, receivedMsg.StatusMessage,
                    receivedMsg.Exception);
            }

            // In the 4.0 format, all results arrive in a single response
            foreach (var handler in _callbackByRequestId.Values)
            {
                handler.HandleReceived(receivedMsg);
                handler.Finalize(new Dictionary<string, object>());
            }
            _callbackByRequestId.Clear();
            DisposeCancellationRegistrations();
        }

        private void BeginSendingMessages()
        {
            if (Interlocked.CompareExchange(ref _writeInProgress, 1, 0) != 0)
                return;
            SendMessagesFromQueueAsync().Forget();
        }

        private async Task SendMessagesFromQueueAsync()
        {
            while (_writeQueue.TryDequeue(out var msg))
            {
                try
                {
                    await SendMessageAsync(msg.msg, msg.cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException e) when (msg.cancellationToken == e.CancellationToken)
                {
                    // Send was cancelled for this message
                }
                catch (Exception e)
                {
                    await CloseConnectionBecauseOfFailureAsync(e).ConfigureAwait(false);
                    break;
                }
            }
            Interlocked.CompareExchange(ref _writeInProgress, 0, 1);

            if (!_writeQueue.IsEmpty && Interlocked.CompareExchange(ref _writeInProgress, 1, 0) == 0)
            {
                await SendMessagesFromQueueAsync().ConfigureAwait(false);
            }
        }

        private async Task CloseConnectionBecauseOfFailureAsync(Exception exception)
        {
            EmptyWriteQueue();
            await CloseAsync().ConfigureAwait(false);
            NotifyAboutConnectionFailure(exception);
        }

        private void EmptyWriteQueue()
        {
            while (_writeQueue.TryDequeue(out _))
            {
            }
        }

        private void NotifyAboutConnectionFailure(Exception exception)
        {
            foreach (var cb in _callbackByRequestId.Values)
            {
                cb.HandleFailure(exception);
            }
            _callbackByRequestId.Clear();
            DisposeCancellationRegistrations();
        }

        private async Task SendMessageAsync(RequestMessage message, CancellationToken cancellationToken)
        {
            var serializedMsg = await _messageSerializer.SerializeMessageAsync(message, cancellationToken)
                .ConfigureAwait(false);
            await _webSocketConnection.SendMessageAsync(serializedMsg, cancellationToken).ConfigureAwait(false);
        }

        public async Task CloseAsync()
        {
            Interlocked.Exchange(ref _connectionState, Closed);
            await _webSocketConnection.CloseAsync().ConfigureAwait(false);
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
                {
                    _webSocketConnection?.Dispose();
                    DisposeCancellationRegistrations();
                }
                _disposed = true;
            }
        }

        private void DisposeCancellationRegistrations()
        {
            foreach (var cancellation in _cancellationByRequestId.Values)
            {
                cancellation.Dispose();
            }
            _cancellationByRequestId.Clear();
        }

        #endregion
    }
}
