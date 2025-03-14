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
using Gremlin.Net.Structure;
using Gremlin.Net.Process.Remote;

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     Provides a method for creating a <see cref="GraphTraversalSource"/> that does not spawn from a
    ///     <see cref="Graph"/> instance. 
    /// </summary>
    public class AnonymousTraversalSource {

        private AnonymousTraversalSource()
        {
        }

        /// <summary>
        ///     Generates a reusable <see cref="GraphTraversalSource" /> instance.
        /// </summary>
        /// <returns>A graph traversal source.</returns>
        public static AnonymousTraversalSource Traversal()
        {
            return new AnonymousTraversalSource();
        }

        /// <summary>
        ///     Configures the <see cref="GraphTraversalSource" /> as a "remote" to issue the
        ///     <see cref="GraphTraversal{SType, EType}" /> for execution elsewhere.
        /// </summary>
        /// <param name="remoteConnection">
        ///     The <see cref="IRemoteConnection" /> instance to use to submit the
        ///     <see cref="GraphTraversal{SType, EType}" />.
        /// </param>
        /// <returns>A <see cref="GraphTraversalSource" /> configured to use the provided <see cref="IRemoteConnection" />.</returns>
        public GraphTraversalSource With(IRemoteConnection remoteConnection) =>
            new GraphTraversalSource(new List<ITraversalStrategy>(),
                new Bytecode(), remoteConnection);
    }

}