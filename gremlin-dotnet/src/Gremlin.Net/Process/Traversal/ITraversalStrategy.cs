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

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     A <see cref="ITraversalStrategy" /> defines a particular atomic operation for mutating a
    ///     <see cref="ITraversal" /> prior to its evaluation.
    /// </summary>
    public interface ITraversalStrategy
    {
        /// <summary>
        ///     Applies the strategy to the given <see cref="ITraversal" />.
        /// </summary>
        /// <param name="traversal">The <see cref="ITraversal" /> the strategy should be applied to.</param>
        void Apply<S, E>(ITraversal<S, E> traversal);

        /// <summary>
        ///     Applies the strategy to the given <see cref="ITraversal" /> asynchronously.
        /// </summary>
        /// <param name="traversal">The <see cref="ITraversal" /> the strategy should be applied to.</param>
        Task ApplyAsync<S, E>(ITraversal<S, E> traversal);
    }
}