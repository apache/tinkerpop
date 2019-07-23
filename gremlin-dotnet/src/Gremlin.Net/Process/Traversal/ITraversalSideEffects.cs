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

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     A <see cref="ITraversal" /> can maintain global sideEffects.
    /// </summary>
    [Obsolete("As of release 3.3.8, not replaced, prefer use of cap()-step to retrieve side-effects as part of traversal iteration", false)]
    public interface ITraversalSideEffects : IDisposable
    {
        /// <summary>
        ///     Retrieves the keys of the side-effect that can be supplied to <see cref="Get" />.
        /// </summary>
        /// <returns>The keys of the side-effect.</returns>
        IReadOnlyCollection<string> Keys();

        /// <summary>
        ///     Gets the side-effect associated with the provided key.
        /// </summary>
        /// <param name="key">The key to get the value for.</param>
        /// <returns>The value associated with key.</returns>
        object Get(string key);

        /// <summary>
        ///     Invalidates the side effect cache for traversal.
        /// </summary>
        void Close();
    }
}