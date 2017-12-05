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
using System.Reflection;
using System.Threading.Tasks;

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     A traversal represents a directed walk over a graph.
    /// </summary>
    public abstract class DefaultTraversal<S, E> : ITraversal<S, E>
    {
        private IEnumerator<Traverser> _traverserEnumerator;

        /// <summary>
        ///     Gets the <see cref="Traversal.Bytecode" /> representation of this traversal.
        /// </summary>
        public Bytecode Bytecode { get; protected set; }

        /// <summary>
        ///     Gets or sets the <see cref="ITraversalSideEffects" /> of this traversal.
        /// </summary>
        public ITraversalSideEffects SideEffects { get; set; }

        /// <summary>
        ///     Gets or sets the <see cref="Traverser" />'s of this traversal that hold the results of the traversal.
        /// </summary>
        public IEnumerable<Traverser> Traversers { get; set; }

        ITraversal ITraversal.Iterate()
        {
            return Iterate();
        }

        /// <summary>
        ///     Gets or sets the <see cref="ITraversalStrategy" /> strategies of this traversal.
        /// </summary>
        protected ICollection<ITraversalStrategy> TraversalStrategies { get; set; } = new List<ITraversalStrategy>();

        private IEnumerator<Traverser> TraverserEnumerator
            => _traverserEnumerator ?? (_traverserEnumerator = GetTraverserEnumerator());

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
        }

        /// <inheritdoc />
        public bool MoveNext()
        {
            var currentTraverser = TraverserEnumerator.Current;
            if (currentTraverser?.Bulk > 1)
            {
                currentTraverser.Bulk--;
                return true;
            }
            return TraverserEnumerator.MoveNext();
        }

        /// <summary>
        ///     Reset is not supported.
        /// </summary>
        /// <exception cref="NotSupportedException">Thrown always as this operation is not supported.</exception>
        public void Reset()
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public E Current => GetCurrent<E>();

        object IEnumerator.Current => GetCurrent();

        private TReturn GetCurrent<TReturn>()
        {
            var value = GetCurrent();
            var returnType = typeof(TReturn);
            if (value == null || value.GetType() == returnType)
            {
                // Avoid evaluating type comparisons
                return (TReturn) value;
            }
            return (TReturn) GetValue(returnType, value);
        }

        private object GetCurrent()
        {
            // Use dynamic to object to prevent runtime dynamic conversion evaluation
            return TraverserEnumerator.Current?.Object;
        }

        /// <summary>
        /// Gets the value, converting to the expected type when necessary and supported. 
        /// </summary>
        private static object GetValue(Type type, object value)
        {
            var genericType = type.GetTypeInfo().IsGenericType
                ? type.GetTypeInfo().GetGenericTypeDefinition()
                : null;
            if (value is IDictionary dictValue && genericType == typeof(IDictionary<,>))
            {
                var keyType = type.GenericTypeArguments[0];
                var valueType = type.GenericTypeArguments[1];
                var mapType = typeof(Dictionary<,>).MakeGenericType(keyType, valueType);
                var result = (IDictionary) Activator.CreateInstance(mapType);
                foreach (DictionaryEntry kv in dictValue)
                {
                    result.Add(GetValue(keyType, kv.Key), GetValue(valueType, kv.Value));
                }
                return result;
            }
            if (value is IEnumerable enumerableValue && genericType == typeof(IList<>))
            {
                var childType = type.GenericTypeArguments[0];
                var listType = typeof(List<>).MakeGenericType(childType);
                var result = (IList) Activator.CreateInstance(listType);
                foreach (var itemValue in enumerableValue)
                {
                    result.Add(itemValue);
                }
                return result;
            }
            return value;
        }

        private IEnumerator<Traverser> GetTraverserEnumerator()
        {
            if (Traversers == null)
                ApplyStrategies();
            return Traversers.GetEnumerator();
        }

        private void ApplyStrategies()
        {
            foreach (var strategy in TraversalStrategies)
                strategy.Apply(this);
        }

        private async Task ApplyStrategiesAsync()
        {
            foreach (var strategy in TraversalStrategies)
                await strategy.ApplyAsync(this).ConfigureAwait(false);
        }

        /// <summary>
        ///     Gets the next result from the traversal.
        /// </summary>
        /// <returns>The result.</returns>
        public E Next()
        {
            MoveNext();
            return Current;
        }

        /// <summary>
        ///     Gets the next n-number of results from the traversal.
        /// </summary>
        /// <param name="amount">The number of results to get.</param>
        /// <returns>The n-results.</returns>
        public IEnumerable<E> Next(int amount)
        {
            for (var i = 0; i < amount; i++)
                yield return Next();
        }

        /// <summary>
        ///     Iterates all <see cref="Traverser" /> instances in the traversal.
        /// </summary>
        /// <returns>The fully drained traversal.</returns>
        public ITraversal<S, E> Iterate()
        {
            while (MoveNext())
            {
            }
            return this;
        }

        /// <summary>
        ///     Gets the next <see cref="Traverser" />.
        /// </summary>
        /// <returns>The next <see cref="Traverser" />.</returns>
        public Traverser NextTraverser()
        {
            TraverserEnumerator.MoveNext();
            return TraverserEnumerator.Current;
        }

        /// <summary>
        ///     Puts all the results into a <see cref="List{T}" />.
        /// </summary>
        /// <returns>The results in a list.</returns>
        public IList<E> ToList()
        {
            var objs = new List<E>();
            while (MoveNext())
                objs.Add(Current);
            return objs;
        }

        /// <summary>
        ///     Puts all the results into a <see cref="HashSet{T}" />.
        /// </summary>
        /// <returns>The results in a set.</returns>
        public ISet<E> ToSet()
        {
            var objs = new HashSet<E>();
            while (MoveNext())
                objs.Add(Current);
            return objs;
        }

        /// <inheritdoc />
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
                SideEffects?.Dispose();
        }

        /// <summary>
        ///     Starts a promise to execute a function on the current traversal that will be completed in the future.
        /// </summary>
        /// <typeparam name="TReturn">The return type of the <paramref name="callback" />.</typeparam>
        /// <param name="callback">The function to execute on the current traversal.</param>
        /// <returns>The result of the executed <paramref name="callback" />.</returns>
        public async Task<TReturn> Promise<TReturn>(Func<ITraversal<S, E>, TReturn> callback)
        {
            await ApplyStrategiesAsync().ConfigureAwait(false);
            return callback(this);
        }
    }
}