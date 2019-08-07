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
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    /// Represents the basic information for a walk over a graph.
    /// </summary>
    /// <seealso cref="ITraversal{SType, EType}"/>
    public interface ITraversal: IEnumerator
    {
        /// <summary>
        ///     Gets the <see cref="Bytecode" /> representation of this traversal.
        /// </summary>
        Bytecode Bytecode { get; }

        /// <summary>
        ///     Gets or sets the <see cref="Traverser" />'s of this traversal that hold the results of the traversal.
        /// </summary>
        IEnumerable<Traverser> Traversers { get; set; }

        /// <summary>
        ///     Iterates all <see cref="Traverser" /> instances in the traversal.
        /// </summary>
        /// <returns>The fully drained traversal.</returns>
        ITraversal Iterate();
    }

    /// <summary>
    ///     A traversal represents a directed walk over a graph.
    /// </summary>
    public interface ITraversal<S, E> : ITraversal, IEnumerator<E>
    {
        /// <summary>
        ///     Gets the next result from the traversal.
        /// </summary>
        /// <returns>The result.</returns>
        E Next();

        /// <summary>
        ///     Determines if the traversal contains any additional results for iteration.
        /// </summary>
        /// <returns>True if there are more results and false otherwise.</returns>
        bool HasNext();

        /// <summary>
        ///     Gets the next n-number of results from the traversal.
        /// </summary>
        /// <param name="amount">The number of results to get.</param>
        /// <returns>The n-results.</returns>
        IEnumerable<E> Next(int amount);

        /// <summary>
        ///     Iterates all <see cref="Traverser" /> instances in the traversal.
        /// </summary>
        /// <returns>The fully drained traversal.</returns>
        new ITraversal<S, E> Iterate();

        /// <summary>
        ///     Gets the next <see cref="Traverser" />.
        /// </summary>
        /// <returns>The next <see cref="Traverser" />.</returns>
        Traverser NextTraverser();

        /// <summary>
        ///     Puts all the results into a <see cref="IList{T}" />.
        /// </summary>
        /// <returns>The results in a list.</returns>
        IList<E> ToList();

        /// <summary>
        ///     Puts all the results into a <see cref="ISet{T}" />.
        /// </summary>
        /// <returns>The results in a set.</returns>
        ISet<E> ToSet();

        /// <summary>
        ///     Starts a promise to execute a function on the current traversal that will be completed in the future.
        /// </summary>
        /// <typeparam name="TReturn">The return type of the <paramref name="callback" />.</typeparam>
        /// <param name="callback">The function to execute on the current traversal.</param>
        /// <returns>The result of the executed <paramref name="callback" />.</returns>
        Task<TReturn> Promise<TReturn>(Func<ITraversal<S, E>, TReturn> callback);
    }
}