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
using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Driver
{
    internal sealed class ProxyConnection : IConnection
    {
        public IConnection ProxiedConnection { get; }
        private readonly Action<IConnection> _releaseAction;

        public ProxyConnection(IConnection proxiedConnection, Action<IConnection> releaseAction)
        {
            ProxiedConnection = proxiedConnection;
            _releaseAction = releaseAction;
        }

        public async Task ConnectAsync()
        {
            await ProxiedConnection.ConnectAsync().ConfigureAwait(false);
        }

        public async Task<ResultSet<T>> SubmitAsync<T>(RequestMessage requestMessage, CancellationToken cancellationToken = default)
        {
            return await ProxiedConnection.SubmitAsync<T>(requestMessage, cancellationToken).ConfigureAwait(false);
        }

        public int NrRequestsInFlight => ProxiedConnection.NrRequestsInFlight;

        public bool IsOpen => ProxiedConnection.IsOpen;

        public async Task CloseAsync()
        {
            await ProxiedConnection.CloseAsync().ConfigureAwait(false);
        }

        public void Dispose()
        {
            _releaseAction(ProxiedConnection);
        }
    }
}