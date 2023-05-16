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
using System.Text;
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
        private readonly bool _sessionEnabled;

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
            if (!string.IsNullOrEmpty(sessionId))
            {
                _sessionEnabled = true;
            }
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
            _callbackByRequestId.GetOrAdd(requestMessage.RequestId, receiver);

            _cancellationByRequestId.GetOrAdd(requestMessage.RequestId, cancellationToken.Register(() =>
            {
                if (_callbackByRequestId.TryRemove(requestMessage.RequestId, out var responseHandler))
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
            if (receivedMsg == null)
            {
                throw new InvalidOperationException(
                    "Received data deserialized into null object message. Cannot operate on it.");
            }

            try
            {
                HandleReceivedMessage(receivedMsg);
            }
            catch (Exception e)
            {
                if (receivedMsg!.RequestId != null)
                {
                    if(_callbackByRequestId.TryRemove(receivedMsg.RequestId.Value, out var responseHandler))
                    {
                        responseHandler?.HandleFailure(e);
                        
                    }
                    if (_cancellationByRequestId.TryRemove(receivedMsg.RequestId.Value, out var cancellation))
                    {
                        cancellation.Dispose();
                    }
                }
            }
        }

        private void HandleReceivedMessage(ResponseMessage<List<object>> receivedMsg)
        {
            var status = receivedMsg.Status;
            status.ThrowIfStatusIndicatesError();

            if (status.Code == ResponseStatusCode.Authenticate)
            {
                Authenticate();
                return;
            }

            if (receivedMsg.RequestId == null) return;

            _callbackByRequestId.TryGetValue(receivedMsg.RequestId.Value, out var responseHandler);
            if (status.Code != ResponseStatusCode.NoContent)
            {
                responseHandler?.HandleReceived(receivedMsg);
            }

            if (status.Code == ResponseStatusCode.Success || status.Code == ResponseStatusCode.NoContent)
            {
                if (_cancellationByRequestId.TryRemove(receivedMsg.RequestId.Value, out var cancellation))
                {
                    cancellation.Dispose();
                }
                responseHandler?.Finalize(status.Attributes);
                _callbackByRequestId.TryRemove(receivedMsg.RequestId.Value, out _);
            }
        }

        private void Authenticate()
        {
            if (string.IsNullOrEmpty(_username) || string.IsNullOrEmpty(_password))
                throw new InvalidOperationException(
                    $"The Gremlin Server requires authentication, but no credentials are specified - username: {_username}, password: {_password}.");

            var message = RequestMessage.Build(Tokens.OpsAuthentication).Processor(Tokens.ProcessorTraversal)
                .AddArgument(Tokens.ArgsSasl, SaslArgument()).Create();

            _writeQueue.Enqueue((message, CancellationToken.None));
            BeginSendingMessages();
        }

        private string SaslArgument()
        {
            var auth = $"\0{_username}\0{_password}";
            var authBytes = Encoding.UTF8.GetBytes(auth);
            return Convert.ToBase64String(authBytes);
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
                    // Send was cancelled for this message -> silently catch as we want to continue sending from this
                    //  connection. The task responsible for submitting this message will be cancelled by the
                    //  `ResponseHandlerForSingleRequestMessage`.
                }
                catch (Exception e)
                {
                    await CloseConnectionBecauseOfFailureAsync(e).ConfigureAwait(false);
                    break;
                }
            }
            Interlocked.CompareExchange(ref _writeInProgress, 0, 1);

            // Since the loop ended and the write in progress was set to 0
            // a new item could have been added, write queue can contain items at this time
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
            if (_sessionEnabled)
            {
                message = RebuildSessionMessage(message);
            }

            var serializedMsg = await _messageSerializer.SerializeMessageAsync(message, cancellationToken)
                .ConfigureAwait(false);
#if NET6_0_OR_GREATER
            if (message.Processor == Tokens.OpsAuthentication)
            {
                // Don't compress a message that contains credentials to prevent attacks like CRIME or BREACH
                await _webSocketConnection.SendMessageUncompressedAsync(serializedMsg, cancellationToken).ConfigureAwait(false);
                return;
            }
#endif
            await _webSocketConnection.SendMessageAsync(serializedMsg, cancellationToken).ConfigureAwait(false);
        }

        private RequestMessage RebuildSessionMessage(RequestMessage message)
        {
            if (message.Processor == Tokens.OpsAuthentication)
            {
                return message;
            }

            var msgBuilder = RequestMessage.Build(message.Operation)
              .OverrideRequestId(message.RequestId).Processor(Tokens.ProcessorSession);
            foreach(var kv in message.Arguments)
            {
                msgBuilder.AddArgument(kv.Key, kv.Value);
            }
            msgBuilder.AddArgument(Tokens.ArgsSession, _sessionId!);
            return msgBuilder.Create();
        }

        public async Task CloseAsync()
        {
            Interlocked.Exchange(ref _connectionState, Closed);

            if (_sessionEnabled)
            {
                await CloseSession().ConfigureAwait(false);
            }
            
            await _webSocketConnection.CloseAsync().ConfigureAwait(false);
        }

        private async Task CloseSession()
        {
            // build a request to close this session
            var msg = RequestMessage.Build(Tokens.OpsClose).Processor(Tokens.ProcessorSession).Create();

            await SendMessageAsync(msg, CancellationToken.None).ConfigureAwait(false);
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