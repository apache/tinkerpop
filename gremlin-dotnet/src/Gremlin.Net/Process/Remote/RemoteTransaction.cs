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
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Process.Remote
{
    /// <summary>
    ///     A controller for a remote transaction that is constructed from <code>g.Tx()</code>. Calling <code>Begin()</code> on
    ///     this object will produce a new <code>GraphTraversalSource</code> that is bound to a remote transaction over which
    ///     multiple traversals may be executed in that context. Calling <code>CommitAsync()</code> or
    ///     <code>RollbackAsync()</code> will then close the transaction and thus, the session. This feature only works with
    ///     transaction enabled graphs.
    /// </summary>
    public class RemoteTransaction
    {
        private readonly IRemoteConnection _sessionBasedConnection;
        private GraphTraversalSource _g;

        /// <summary>
        ///     Initializes a new instance of the <see cref="RemoteTransaction" /> class.
        /// </summary>
        /// <param name="connection">The session bound connection that will be used to control this transaction.</param>
        /// <param name="g">The graph traversal source from which a session bound traversal source will be created.</param>
        public RemoteTransaction(IRemoteConnection connection, GraphTraversalSource g)
        {
            _sessionBasedConnection = connection;
            _g = g;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversalSource" /> that is bound to a remote session which enables a transaction.
        /// </summary>
        /// <returns>A <see cref="GraphTraversalSource" /> bound to a remote session.</returns>
        /// <exception cref="InvalidOperationException">Thrown if this transaction is already bound to a session.</exception>
        public GraphTraversalSource Begin()
        {
            if (_g.IsSessionBound)
            {
                throw new InvalidOperationException("Transaction already started on this object");
            }
            _g = new GraphTraversalSource(_g.TraversalStrategies, _g.Bytecode, _sessionBasedConnection);
            return _g;
        }

        /// <summary>
        ///     Commits the transaction.
        /// </summary>
        public async Task CommitAsync()
        {
            await _sessionBasedConnection.SubmitAsync<object, object>(GraphOp.Commit).ConfigureAwait(false);
        }

        /// <summary>
        ///     Rolls back the transaction.
        /// </summary>
        public async Task RollbackAsync()
        {
            await _sessionBasedConnection.SubmitAsync<object, object>(GraphOp.Rollback).ConfigureAwait(false);
        }
    }
}