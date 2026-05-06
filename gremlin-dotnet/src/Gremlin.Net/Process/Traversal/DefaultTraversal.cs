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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     A traversal represents a directed walk over a graph.
    /// </summary>
    public abstract class DefaultTraversal<TStart, TEnd> : ITraversal<TStart, TEnd>
    {
        private IAsyncEnumerator<Traverser>? _traverserEnumerator;
        private bool _nextAvailable;
        private bool _fetchedNext;

        /// <summary>
        ///     Gets the <see cref="Traversal.GremlinLang" /> representation of this traversal.
        /// </summary>
        public abstract GremlinLang GremlinLang { get; }

        /// <summary>
        ///     Determines if this traversal was spawned anonymously or not.
        /// </summary>
        public bool IsAnonymous { get; protected set; }

        /// <summary>
        ///     Gets or sets the <see cref="Traverser" />'s of this traversal that hold the results of the traversal.
        /// </summary>
        public IAsyncEnumerable<Traverser>? Traversers { get; set; }

        /// <summary>
        ///     Gets or sets the <see cref="ITraversalStrategy" /> strategies of this traversal.
        /// </summary>
        protected ICollection<ITraversalStrategy> TraversalStrategies { get; set; } = new List<ITraversalStrategy>();

        private IAsyncEnumerator<Traverser> TraverserEnumerator
            => _traverserEnumerator ??= GetTraverserEnumerator();

        /// <inheritdoc />
        public TEnd? Current => GetCurrent<TEnd>();

        /// <inheritdoc />
        public object? CurrentObject => GetValue();

        /// <inheritdoc />
        public async ValueTask<bool> MoveNextAsync()
        {
            var more = await MoveNextInternalAsync().ConfigureAwait(false);
            _fetchedNext = false;
            return more;
        }

        private async ValueTask<bool> MoveNextInternalAsync()
        {
            if (_fetchedNext) return _nextAvailable;

            if (!_nextAvailable || TraverserEnumerator.Current?.Bulk == 0)
            {
                _nextAvailable = await TraverserEnumerator.MoveNextAsync()
                    .ConfigureAwait(false);
            }
            if (!_nextAvailable) return false;

            var currentTraverser = TraverserEnumerator.Current;
            if (currentTraverser?.Bulk >= 1)
            {
                currentTraverser.Bulk--;
            }
            return true;
        }

        /// <inheritdoc />
        public bool HasNext()
        {
            var more = MoveNextInternalAsync().AsTask().WaitUnwrap();
            _fetchedNext = true;
            return more;
        }

        /// <summary>
        ///     Gets the next result from the traversal.
        /// </summary>
        /// <returns>The result.</returns>
        public TEnd? Next()
        {
            MoveNextAsync().AsTask().WaitUnwrap();
            return Current;
        }

        /// <summary>
        ///     Gets the next n-number of results from the traversal.
        /// </summary>
        /// <param name="amount">The number of results to get.</param>
        /// <returns>The n-results.</returns>
        public IEnumerable<TEnd?> Next(int amount)
        {
            for (var i = 0; i < amount; i++)
                yield return Next();
        }

        /// <summary>
        ///     Puts all the results into a <see cref="IList{T}" />.
        /// </summary>
        /// <returns>The results in a list.</returns>
        public IList<TEnd?> ToList()
        {
            var objs = new List<TEnd?>();
            while (MoveNextAsync().AsTask().WaitUnwrap())
                objs.Add(Current);
            return objs;
        }

        /// <summary>
        ///     Puts all the results into a <see cref="HashSet{T}" />.
        /// </summary>
        /// <returns>The results in a set.</returns>
        public ISet<TEnd?> ToSet()
        {
            var objs = new HashSet<TEnd?>();
            while (MoveNextAsync().AsTask().WaitUnwrap())
                objs.Add(Current);
            return objs;
        }

        /// <summary>
        ///     Iterates all <see cref="Traverser" /> instances in the traversal.
        /// </summary>
        /// <returns>The fully drained traversal.</returns>
        public ITraversal<TStart, TEnd> Iterate()
        {
            GremlinLang.AddStep("discard");
            IterateAsync().WaitUnwrap();
            return this;
        }

        /// <summary>
        ///     Iterates all <see cref="Traverser" /> instances in the traversal asynchronously.
        /// </summary>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The fully drained traversal.</returns>
        public async Task<ITraversal<TStart, TEnd>> IterateAsync(
            CancellationToken cancellationToken = default)
        {
            while (await MoveNextAsync().ConfigureAwait(false)) { }
            return this;
        }

        ITraversal ITraversal.Iterate() => Iterate();

        /// <summary>
        ///     Gets the next <see cref="Traverser" />.
        /// </summary>
        /// <returns>The next <see cref="Traverser" />.</returns>
        public Traverser NextTraverser()
        {
            TraverserEnumerator.MoveNextAsync().AsTask().WaitUnwrap();
            return TraverserEnumerator.Current;
        }

        /// <summary>
        ///     Starts a promise to execute a function on the current traversal that will be completed in the future.
        /// </summary>
        /// <typeparam name="TReturn">The return type of the <paramref name="callback" />.</typeparam>
        /// <param name="callback">The function to execute on the current traversal.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The result of the executed <paramref name="callback" />.</returns>
        public async Task<TReturn> Promise<TReturn>(Func<ITraversal<TStart, TEnd>, TReturn> callback,
            CancellationToken cancellationToken = default)
        {
            await ApplyStrategiesAsync(cancellationToken).ConfigureAwait(false);
            return callback(this);
        }

        /// <summary>
        ///     Disposes the async enumerator used by this traversal.
        /// </summary>
        public ValueTask DisposeAsync()
        {
            return _traverserEnumerator?.DisposeAsync() ?? ValueTask.CompletedTask;
        }

        private TReturn? GetCurrent<TReturn>()
        {
            var value = GetValue();
            var returnType = typeof(TReturn);
            if (value == null || value.GetType() == returnType)
            {
                // Avoid evaluating type comparisons
                return (TReturn?) value;
            }
            return (TReturn) GetValue(returnType, value);
        }

        private object? GetValue()
        {
            return TraverserEnumerator.Current?.Object;
        }

        /// <summary>
        ///     Converts a value to the expected type when necessary and supported.
        /// </summary>
        [return: NotNullIfNotNull("value")]
        private static object? GetValue(Type type, object? value)
        {
            var genericType = type.GetTypeInfo().IsGenericType
                ? type.GetTypeInfo().GetGenericTypeDefinition()
                : null;
            if (value is IDictionary dictValue && genericType == typeof(IDictionary<,>))
            {
                var keyType = type.GenericTypeArguments[0];
                var valueType = type.GenericTypeArguments[1];
                var mapType = typeof(Dictionary<,>).MakeGenericType(keyType, valueType);
                var result = (IDictionary?)Activator.CreateInstance(mapType) ??
                             throw new InvalidOperationException($"Cannot convert value {value} to a Dictionary.");
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
                var result = (IList?)Activator.CreateInstance(listType) ??
                             throw new InvalidOperationException($"Cannot convert value {value} to a list.");
                foreach (var itemValue in enumerableValue)
                {
                    result.Add(itemValue);
                }
                return result;
            }
            return value;
        }

        private IAsyncEnumerator<Traverser> GetTraverserEnumerator()
        {
            if (Traversers == null)
                ApplyStrategies();
            if (Traversers == null)
            {
                throw new InvalidOperationException(
                    $"Cannot enumerate the traversal as there are no {nameof(Traversers)}.");
            }
            return Traversers.GetAsyncEnumerator();
        }

        private void ApplyStrategies()
        {
            foreach (var strategy in TraversalStrategies)
                strategy.Apply(this);
        }

        private async Task ApplyStrategiesAsync(CancellationToken cancellationToken)
        {
            foreach (var strategy in TraversalStrategies)
                await strategy.ApplyAsync(this, cancellationToken).ConfigureAwait(false);
        }
    }
}
