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
        private readonly WebSocketConnection _webSocketConnection = new WebSocketConnection();
        private readonly string _username;
        private readonly string _password;

        public Connection(Uri uri, string username, string password, GraphSONReader graphSONReader,
                          GraphSONWriter graphSONWriter, string mimeType)
        {
            _uri = uri;
            _username = username;
            _password = password;
            _graphSONReader = graphSONReader;
            _graphSONWriter = graphSONWriter;
            _messageSerializer = new JsonMessageSerializer(mimeType);
        }

        public async Task<IReadOnlyCollection<T>> SubmitAsync<T>(RequestMessage requestMessage)
        {
            await SendAsync(requestMessage).ConfigureAwait(false);
            return await ReceiveAsync<T>().ConfigureAwait(false);
        }

        public Task ConnectAsync()
        {
            return _webSocketConnection.ConnectAsync(_uri);
        }

        public Task CloseAsync()
        {
            return _webSocketConnection.CloseAsync();
        }

        public bool IsOpen => _webSocketConnection.IsOpen;

        private Task SendAsync(RequestMessage message)
        {
            var graphsonMsg = _graphSONWriter.WriteObject(message);
            var serializedMsg = _messageSerializer.SerializeMessage(graphsonMsg);
            return _webSocketConnection.SendMessageAsync(serializedMsg);
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
                    var receivedData = _graphSONReader.ToObject(receivedMsg.Result.Data);
                    foreach (var d in receivedData)
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
                }
            } while (status.Code == ResponseStatusCode.PartialContent || status.Code == ResponseStatusCode.Authenticate);

            if (isAggregatingSideEffects)
                return new List<T> {(T) aggregator.GetAggregatedResult()};
            return result;
        }

        private Task AuthenticateAsync()
        {
            if (string.IsNullOrEmpty(_username) || string.IsNullOrEmpty(_password))
                throw new InvalidOperationException(
                    $"The Gremlin Server requires authentication, but no credentials are specified - username: {_username}, password: {_password}.");

            var message = RequestMessage.Build(Tokens.OpsAuthentication).Processor(Tokens.ProcessorTraversal)
                .AddArgument(Tokens.ArgsSasl, SaslArgument()).Create();

            return SendAsync(message);
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