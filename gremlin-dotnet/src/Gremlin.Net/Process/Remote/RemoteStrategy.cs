﻿#region License

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

using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Process.Remote
{
    /// <summary>
    ///     Reconstructs a <see cref="ITraversal" /> by submitting it to a remote server via an
    ///     <see cref="IRemoteConnection" /> instance.
    /// </summary>
    public class RemoteStrategy : ITraversalStrategy
    {
        private readonly IRemoteConnection _remoteConnection;

        /// <summary>
        ///     Initializes a new instance of the <see cref="RemoteStrategy" /> class.
        /// </summary>
        /// <param name="remoteConnection">The <see cref="IRemoteConnection" /> that should be used.</param>
        public RemoteStrategy(IRemoteConnection remoteConnection)
        {
            _remoteConnection = remoteConnection;
        }

        /// <inheritdoc />
        public void Apply<TStart, TEnd>(ITraversal<TStart, TEnd> traversal)
        {
            ApplyAsync(traversal, CancellationToken.None).WaitUnwrap();
        }

        /// <inheritdoc />
        public async Task ApplyAsync<TStart, TEnd>(ITraversal<TStart, TEnd> traversal, CancellationToken cancellationToken = default)
        {
            if (traversal.Traversers != null) return;
            var remoteTraversal = await _remoteConnection.SubmitAsync<TStart, TEnd>(traversal.Bytecode, cancellationToken)
                .ConfigureAwait(false);
            traversal.Traversers = remoteTraversal.Traversers;
        }
    }
}