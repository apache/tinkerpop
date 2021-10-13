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

using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Process.Remote
{
    /// <summary>
    ///     A simple abstraction of a "connection" to a "server".
    /// </summary>
    public interface IRemoteConnection
    {
        /// <summary>
        ///     Submits <see cref="ITraversal" /> <see cref="Bytecode" /> to a server and returns a
        ///     <see cref="ITraversal" />.
        /// </summary>
        /// <param name="bytecode">The <see cref="Bytecode" /> to send.</param>
        /// <returns>The <see cref="ITraversal" /> with the results and optional side-effects.</returns>
        Task<ITraversal<S, E>> SubmitAsync<S, E>(Bytecode bytecode);

        /// <summary>
        ///     Creates a <see cref="RemoteTransaction" /> in the context of a <see cref="GraphTraversalSource" /> designed to work with
        ///     remote semantics.
        /// </summary>
        /// <param name="graphTraversalSource">
        ///     The <see cref="GraphTraversalSource" /> providing the context for the
        ///     <see cref="RemoteTransaction" />.
        /// </param>
        /// <returns>The created <see cref="RemoteTransaction" />.</returns>
        RemoteTransaction Tx(GraphTraversalSource graphTraversalSource);
        
        /// <summary>
        ///     Determines if the connection is bound to a session.
        /// </summary>
        bool IsSessionBound { get; }
    }
}