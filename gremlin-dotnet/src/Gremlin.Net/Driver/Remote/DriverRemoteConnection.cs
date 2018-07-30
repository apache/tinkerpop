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
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Driver.Remote
{
    /// <summary>
    ///     A <see cref="IRemoteConnection" /> implementation for Gremlin Server.
    /// </summary>
    public class DriverRemoteConnection : IRemoteConnection, IDisposable
    {
        private readonly IGremlinClient _client;
        private readonly string _traversalSource;

        /// <summary>
        ///     Initializes a new <see cref="IRemoteConnection" />.
        /// </summary>
        /// <param name="client">The <see cref="IGremlinClient" /> that will be used for the connection.</param>
        /// <exception cref="ArgumentNullException">Thrown when client is null.</exception>
        public DriverRemoteConnection(IGremlinClient client):this(client, "g")
        {
        }

        /// <summary>
        ///     Initializes a new <see cref="IRemoteConnection" />.
        /// </summary>
        /// <param name="client">The <see cref="IGremlinClient" /> that will be used for the connection.</param>
        /// <param name="traversalSource">The name of the traversal source on the server to bind to.</param>
        /// <exception cref="ArgumentNullException">Thrown when client is null.</exception>
        public DriverRemoteConnection(IGremlinClient client, string traversalSource)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _traversalSource = traversalSource ?? throw new ArgumentNullException(nameof(traversalSource));
        }

        /// <summary>
        ///     Submits <see cref="Bytecode" /> for evaluation to a remote Gremlin Server.
        /// </summary>
        /// <param name="bytecode">The <see cref="Bytecode" /> to submit.</param>
        /// <returns>A <see cref="ITraversal" /> allowing to access the results and side-effects.</returns>
        public async Task<ITraversal<S, E>> SubmitAsync<S, E>(Bytecode bytecode)
        {
            var requestId = Guid.NewGuid();
            var resultSet = await SubmitBytecodeAsync(requestId, bytecode).ConfigureAwait(false);
            return new DriverRemoteTraversal<S, E>(_client, requestId, resultSet);
        }

        private Task<IReadOnlyCollection<Traverser>> SubmitBytecodeAsync(Guid requestid, Bytecode bytecode)
        {
            var requestMsg =
                RequestMessage.Build(Tokens.OpsBytecode)
                    .Processor(Tokens.ProcessorTraversal)
                    .OverrideRequestId(requestid)
                    .AddArgument(Tokens.ArgsGremlin, bytecode)
                    .AddArgument(Tokens.ArgsAliases, new Dictionary<string, string> {{"g", _traversalSource}})
                    .Create();
            return _client.SubmitAsync<Traverser>(requestMsg);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}