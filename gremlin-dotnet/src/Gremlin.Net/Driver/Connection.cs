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
using System.Text;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Driver.ResultsAggregation;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json.Linq;

namespace Gremlin.Net.Driver
{
    internal class Connection : IConnection
    {
        private readonly GraphSONReader _graphSONReader;
        private readonly GraphSONWriter _graphSONWriter;
        private readonly JsonMessageSerializer _messageSerializer;
        private readonly Uri _uri;
        private readonly WebSocketConnection _webSocketConnection;
        private readonly string _username;
        private readonly string _password;
        private readonly string _sessionId;
        private readonly bool _sessionEnabled;

        public Connection(Uri uri, string username, string password, GraphSONReader graphSONReader,
            GraphSONWriter graphSONWriter, string mimeType,
            Action<ClientWebSocketOptions> webSocketConfiguration, string sessionId)
        {
            _uri = uri;
            _username = username;
            _password = password;
            _sessionId = sessionId;
            if (!string.IsNullOrEmpty(sessionId))
            {
                _sessionEnabled = true;
            }
            _graphSONReader = graphSONReader;
            _graphSONWriter = graphSONWriter;
            _messageSerializer = new JsonMessageSerializer(mimeType);
            _webSocketConnection = new WebSocketConnection(webSocketConfiguration);
        }

        public async Task<IReadOnlyCollection<T>> SubmitAsync<T>(RequestMessage requestMessage)
        {
            await SendAsync(requestMessage).ConfigureAwait(false);
            return await ReceiveAsync<T>().ConfigureAwait(false);
        }

        public async Task ConnectAsync()
        {
            await _webSocketConnection.ConnectAsync(_uri).ConfigureAwait(false);
        }

        public async Task CloseAsync()
        {
            if (_sessionEnabled)
            {
                await CloseSession().ConfigureAwait(false);
            }
            await _webSocketConnection.CloseAsync().ConfigureAwait(false);
        }

        private async Task CloseSession()
        {
            // build a request to close this session, 'gremlin' in args as tips and not be executed actually
            var msg = RequestMessage.Build(Tokens.OpsClose).Processor(Tokens.ProcessorSession)
              .AddArgument(Tokens.ArgsGremlin, "session.close()").Create();

            await SendAsync(msg).ConfigureAwait(false);
        }

        public bool IsOpen => _webSocketConnection.IsOpen;

        private async Task SendAsync(RequestMessage message)
        {
            var msg = message;
            if (_sessionEnabled)
            {
                msg = RebuildSessionMessage(message);
            }
            var graphsonMsg = _graphSONWriter.WriteObject(msg);
            var serializedMsg = _messageSerializer.SerializeMessage(graphsonMsg);
            await _webSocketConnection.SendMessageAsync(serializedMsg).ConfigureAwait(false);
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
            msgBuilder.AddArgument(Tokens.ArgsSession, _sessionId);
            return msgBuilder.Create();
        }

        private async Task<IReadOnlyCollection<T>> ReceiveAsync<T>()
        {
            ResponseStatus status;
            IAggregator aggregator = null;
            var isAggregatingSideEffects = false;
            var result = new List<T>();
            do
            {
                var received = await _webSocketConnection.ReceiveMessageAsync().ConfigureAwait(false);
                var receivedMsg = _messageSerializer.DeserializeMessage<ResponseMessage<JToken>>(received);

                status = receivedMsg.Status;
                status.ThrowIfStatusIndicatesError();

                if (status.Code == ResponseStatusCode.Authenticate)
                {
                    await AuthenticateAsync().ConfigureAwait(false);
                }
                else if (status.Code != ResponseStatusCode.NoContent)
                {
                    var receivedData = typeof(T) == typeof(JToken)
                        ? new[] { receivedMsg.Result.Data }
                        : _graphSONReader.ToObject(receivedMsg.Result.Data);

                    foreach (var d in receivedData)
                        #pragma warning disable 612,618
                        if (receivedMsg.Result.Meta.ContainsKey(Tokens.ArgsSideEffectKey))
                        {
                            if (aggregator == null)
                                aggregator =
                                    new AggregatorFactory().GetAggregatorFor(
                                        (string) receivedMsg.Result.Meta[Tokens.ArgsAggregateTo]);
                            aggregator.Add(d);
                            isAggregatingSideEffects = true;
                        }
                        else
                        {
                            result.Add(d);
                        }
                        #pragma warning disable 612,618
                }
            } while (status.Code == ResponseStatusCode.PartialContent || status.Code == ResponseStatusCode.Authenticate);

            if (isAggregatingSideEffects)
                return new List<T> {(T) aggregator.GetAggregatedResult()};
            return result;
        }

        private async Task AuthenticateAsync()
        {
            if (string.IsNullOrEmpty(_username) || string.IsNullOrEmpty(_password))
                throw new InvalidOperationException(
                    $"The Gremlin Server requires authentication, but no credentials are specified - username: {_username}, password: {_password}.");

            var message = RequestMessage.Build(Tokens.OpsAuthentication).Processor(Tokens.ProcessorTraversal)
                .AddArgument(Tokens.ArgsSasl, SaslArgument()).Create();

            await SendAsync(message).ConfigureAwait(false);
        }

        private string SaslArgument()
        {
            var auth = $"\0{_username}\0{_password}";
            var authBytes = Encoding.UTF8.GetBytes(auth);
            return Convert.ToBase64String(authBytes);
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
                    _webSocketConnection?.Dispose();
                _disposed = true;
            }
        }

        #endregion
    }
}