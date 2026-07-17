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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Gremlin.Net.Structure;

namespace Gremlin.Net.Process.Traversal
{
#pragma warning disable 1591

    /// <summary>
    ///     A <see cref="GraphTraversalSource" /> is the primary DSL of the Gremlin traversal machine.
    ///     It provides access to all the configurations and steps for Turing complete graph computing.
    /// </summary>
    public class GraphTraversalSource
    {
        private readonly IRemoteConnection? _connection;
        
        /// <summary>
        ///     Gets or sets the traversal strategies associated with this graph traversal source.
        /// </summary>
        public ICollection<ITraversalStrategy> TraversalStrategies { get; set; }

        /// <summary>
        ///     Gets or sets the <see cref="Traversal.GremlinLang" /> associated with the current state of this graph traversal
        ///     source.
        /// </summary>
        public GremlinLang GremlinLang { get; set; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversalSource" /> class.
        /// </summary>
        public GraphTraversalSource()
            : this(new List<ITraversalStrategy>(), new GremlinLang())
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversalSource" /> class.
        /// </summary>
        /// <param name="traversalStrategies">The traversal strategies associated with this graph traversal source.</param>
        /// <param name="gremlinLang">
        ///     The <see cref="Traversal.GremlinLang" /> associated with the current state of this graph traversal
        ///     source.
        /// </param>
        public GraphTraversalSource(ICollection<ITraversalStrategy> traversalStrategies, GremlinLang gremlinLang)
        {
            TraversalStrategies = traversalStrategies;
            GremlinLang = gremlinLang;
        }

        public GraphTraversalSource(ICollection<ITraversalStrategy> traversalStrategies,
            GremlinLang gremlinLang, IRemoteConnection connection)
            : this(traversalStrategies.Where(strategy => strategy.GetType() != typeof(RemoteStrategy)).ToList(),
                gremlinLang)
        {
            _connection = connection;
            TraversalStrategies.Add(new RemoteStrategy(connection));
        }

        public GraphTraversalSource With(string key)
        {
            return With(key, true);
        }

        public GraphTraversalSource With(string key, object? value)
        {
            var existingOptions = GremlinLang.OptionsStrategies;
            OptionsStrategy optionsStrategy;

            if (existingOptions.Count == 0)
            {
                optionsStrategy = new OptionsStrategy();
                optionsStrategy.Configuration[key] = value;
                return WithStrategies(optionsStrategy);
            }

            optionsStrategy = existingOptions[existingOptions.Count - 1];
            optionsStrategy.Configuration[key] = value;
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                GremlinLang.Clone());
            // Render multilabel/singlelabel in gremlin text (temporary until these options are removed)
            if (key == "multilabel" || key == "singlelabel")
            {
                source.GremlinLang.Append($".with('{key}')");
            }
            return source;
        }


        public GraphTraversalSource WithBulk(bool useBulk)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withBulk", useBulk);
            return source;
        }

        public GraphTraversalSource WithPath()
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withPath");
            return source;
        }

        public GraphTraversalSource WithSack(object? initialValue)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSack", initialValue);
            return source;
        }

        public GraphTraversalSource WithSack(object? initialValue, IBinaryOperator? mergeOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSack", initialValue, mergeOperator);
            return source;
        }

        public GraphTraversalSource WithSack(object? initialValue, IUnaryOperator? splitOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSack", initialValue, splitOperator);
            return source;
        }

        public GraphTraversalSource WithSack(object? initialValue, IUnaryOperator? splitOperator,
            IBinaryOperator? mergeOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSack", initialValue, splitOperator, mergeOperator);
            return source;
        }

        public GraphTraversalSource WithSack(ISupplier? initialValue)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSack", initialValue);
            return source;
        }

        public GraphTraversalSource WithSack(ISupplier? initialValue, IBinaryOperator? mergeOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSack", initialValue, mergeOperator);
            return source;
        }

        public GraphTraversalSource WithSack(ISupplier? initialValue, IUnaryOperator? splitOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSack", initialValue, splitOperator);
            return source;
        }

        public GraphTraversalSource WithSack(ISupplier? initialValue, IUnaryOperator? splitOperator,
            IBinaryOperator? mergeOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSack", initialValue, splitOperator, mergeOperator);
            return source;
        }

        public GraphTraversalSource WithSideEffect(string? key, object? initialValue)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSideEffect", key, initialValue);
            return source;
        }

        public GraphTraversalSource WithSideEffect(string? key, object? initialValue, IBinaryOperator reducer)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSideEffect", key, initialValue, reducer);
            return source;
        }

        public GraphTraversalSource WithSideEffect(string? key, ISupplier? initialValue)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSideEffect", key, initialValue);
            return source;
        }

        public GraphTraversalSource WithSideEffect(string? key, ISupplier? initialValue, IBinaryOperator? reducer)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            source.GremlinLang.AddSource("withSideEffect", key, initialValue, reducer);
            return source;
        }

        public GraphTraversalSource WithStrategies(params ITraversalStrategy[] traversalStrategies)
        {
            if (traversalStrategies == null) throw new ArgumentNullException(nameof(traversalStrategies));
            
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            var args = new List<object>(traversalStrategies.Length);
            args.AddRange(traversalStrategies);
            source.GremlinLang.AddSource("withStrategies", args.ToArray());
            return source;
        }

        public GraphTraversalSource WithoutStrategies(params Type?[] traversalStrategyClasses)
        {
            if (traversalStrategyClasses == null) throw new ArgumentNullException(nameof(traversalStrategyClasses));
            
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  GremlinLang.Clone());
            var args = new List<object?>(traversalStrategyClasses.Length);
            args.AddRange(traversalStrategyClasses);
            source.GremlinLang.AddSource("withoutStrategies", args.ToArray());
            return source;
        }

        [Obsolete("Use GValue instead.", false)]
        public GraphTraversalSource WithBindings(object? bindings)
        {
            return this;
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
        [Obsolete("Prefer use of AnonymousTraversalSource.with().", false)]
        public GraphTraversalSource WithRemote(IRemoteConnection remoteConnection) =>
            new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                GremlinLang.Clone(), remoteConnection);

        /// <summary>
        ///     Returns the <see cref="RemoteTransaction"/> for this traversal source. If the source is
        ///     already bound to a transaction, the existing transaction is returned.
        /// </summary>
        /// <returns>The transaction.</returns>
        public RemoteTransaction Tx()
        {
            return _connection!.Tx(this);
        }

        /// <summary>
        ///     Runs a unit of work inside a transaction whose lifecycle is managed automatically.
        ///     A transaction is started via <c>this.Tx().BeginAsync()</c>, the supplied
        ///     <paramref name="txWork"/> is invoked with the transaction-bound
        ///     <see cref="GraphTraversalSource"/> (<c>gtx</c>), and the transaction is then committed
        ///     on normal completion or rolled back on any failure.
        ///
        ///     This is a single-shot wrapper (no retry): exactly one
        ///     begin → run → commit/rollback sequence is performed. Only <c>gtx</c> is in scope inside
        ///     the body; the non-transactional source must not be used. If <paramref name="txWork"/>
        ///     throws, the transaction is rolled back and the original exception is re-thrown unchanged.
        ///     If the commit fails, a rollback is attempted for server-side hygiene and the commit error
        ///     is propagated. A failed rollback during cleanup is swallowed so it never replaces the
        ///     primary (body or commit) error.
        /// </summary>
        /// <param name="txWork">
        ///     The unit of work to run against the transaction-bound <c>gtx</c>.
        /// </param>
        /// <param name="cancellationToken">The token to cancel the operation.</param>
        /// <returns>A <see cref="Task"/> that completes when the transaction has been committed.</returns>
        public async Task ExecuteInTxAsync(Func<GraphTraversalSource, Task> txWork,
            CancellationToken cancellationToken = default)
            => await EvaluateInTxAsync<object?>(async gtx =>
            {
                await txWork(gtx).ConfigureAwait(false);
                return null;
            }, cancellationToken).ConfigureAwait(false);

        /// <summary>
        ///     Runs a value-returning unit of work inside a transaction whose lifecycle is managed
        ///     automatically. A transaction is started via <c>this.Tx().BeginAsync()</c>, the supplied
        ///     <paramref name="txWork"/> is invoked with the transaction-bound
        ///     <see cref="GraphTraversalSource"/> (<c>gtx</c>), and the transaction is then committed
        ///     on normal completion or rolled back on any failure.
        ///
        ///     This is a single-shot wrapper (no retry): exactly one
        ///     begin → run → commit/rollback sequence is performed. Only <c>gtx</c> is in scope inside
        ///     the body; the non-transactional source must not be used. The value produced by
        ///     <paramref name="txWork"/> is returned to the caller after a successful commit. If
        ///     <paramref name="txWork"/> throws, the transaction is rolled back and the original
        ///     exception is re-thrown unchanged. If the commit fails, a rollback is attempted for
        ///     server-side hygiene and the commit error is propagated. A failed rollback during cleanup
        ///     is swallowed so it never replaces the primary (body or commit) error.
        /// </summary>
        /// <typeparam name="T">The type of value produced by <paramref name="txWork"/>.</typeparam>
        /// <param name="txWork">
        ///     The unit of work to run against the transaction-bound <c>gtx</c>. Its result is returned
        ///     to the caller once the transaction commits.
        /// </param>
        /// <param name="cancellationToken">The token to cancel the operation.</param>
        /// <returns>The value produced by <paramref name="txWork"/>.</returns>
        public async Task<T> EvaluateInTxAsync<T>(Func<GraphTraversalSource, Task<T>> txWork,
            CancellationToken cancellationToken = default)
        {
            var tx = this.Tx();
            var gtx = await tx.BeginAsync(cancellationToken).ConfigureAwait(false);
            T result;
            // Phase 1: run the user's work. If it throws, roll back and rethrow the body error - the
            // throw below exits the method, so a failed body never reaches the commit in phase 2.
            try
            {
                result = await txWork(gtx).ConfigureAwait(false);
            }
            catch (Exception)
            {
                try
                {
                    // No cancellation token: if the body failed because the token was cancelled,
                    // honoring it here would abort the cleanup rollback before it is sent.
                    await tx.RollbackAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Rollback cleanup failure is swallowed so the original body error stays primary.
                }
                throw;
            }

            // Phase 2: the body succeeded, so commit. A separate try because this failure mode is
            // distinct (commit, not body): we still roll back for server-side hygiene, then rethrow
            // the commit error as the primary error.
            try
            {
                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                try
                {
                    // No cancellation token: honoring a cancelled token here would abort the
                    // cleanup rollback before it is sent, leaving the transaction open server-side.
                    await tx.RollbackAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Rollback cleanup failure is swallowed so the commit error stays primary.
                }
                throw;
            }

            return result;
        }

        /// <summary>
        ///     Add a GraphComputer class used to execute the traversal.
        ///     This adds a <see cref="VertexProgramStrategy" /> to the strategies.
        /// </summary>
        public GraphTraversalSource WithComputer(string? graphComputer = null, int? workers = null,
            string? persist = null, string? result = null, ITraversal? vertices = null, ITraversal? edges = null,
            Dictionary<string, dynamic>? configuration = null)
        {
            return WithStrategies(new VertexProgramStrategy(graphComputer, workers, persist, result, vertices, edges,
                configuration));
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the E step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> E(params object?[]? edgesIds)
        {
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, GremlinLang.Clone());
            if (edgesIds == null)
            {
                traversal.GremlinLang.AddStep("E", new object?[] { null });
            }
            else
            {
                var args = new List<object?>(edgesIds.Length);
                args.AddRange(edgesIds);
                traversal.GremlinLang.AddStep("E", args.ToArray());
            }
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the V step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> V(params object?[]? vertexIds)
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, GremlinLang.Clone());
            if (vertexIds == null)
            {
                traversal.GremlinLang.AddStep("V", new object?[] { null });
            }
            else
            {
                for (int i = 0; i < vertexIds.Length; i++)
                {
                    if (vertexIds[i] is Vertex vertex)
                    {
                        vertexIds[i] = vertex.Id;
                    }
                }
                var args = new List<object?>(vertexIds.Length);
                args.AddRange(vertexIds);
                traversal.GremlinLang.AddStep("V", args.ToArray());
            }
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addE step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> AddE(string label)
        {
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("addE", label);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addE step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> AddE(GValue<string> label)
        {
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("addE", label);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addE step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> AddE(ITraversal edgeLabelTraversal)
        {
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("addE", edgeLabelTraversal);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addE step
        ///     with multiple labels to that traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> AddE(string label1, string label2, params string[] moreLabels)
        {
            if (label1 == null) throw new ArgumentNullException(nameof(label1));
            if (label2 == null) throw new ArgumentNullException(nameof(label2));
            if (moreLabels == null) throw new ArgumentNullException(nameof(moreLabels));

            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, GremlinLang.Clone());
            var args = new List<object>(2 + moreLabels.Length) { label1, label2 };
            args.AddRange(moreLabels);
            traversal.GremlinLang.AddStep("addE", args.ToArray());
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the mergeE step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> MergeE(IDictionary<object,object?>? m)
        {
            if (m != null && m.ContainsKey(Direction.Out) && m[Direction.Out] is Vertex outV) {
                m[Direction.Out] = outV.Id;
            }
            if (m != null && m.ContainsKey(Direction.In) && m[Direction.In] is Vertex inV) {
                m[Direction.In] = inV.Id;
            }
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("mergeE", m);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the mergeE step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> MergeE(GValue<IDictionary<object, object>> searchCreate)
        {
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("mergeE", searchCreate);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the mergeE step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> MergeE(ITraversal? t)
        {
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("mergeE", t);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> AddV()
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("addV");
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> AddV(string label, params string[] additionalLabels)
        {
            if (label == null) throw new ArgumentNullException(nameof(label));
            if (additionalLabels == null) throw new ArgumentNullException(nameof(additionalLabels));

            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, GremlinLang.Clone());
            if (additionalLabels.Length == 0)
            {
                traversal.GremlinLang.AddStep("addV", label);
            }
            else
            {
                var args = new List<object>(1 + additionalLabels.Length) { label };
                args.AddRange(additionalLabels);
                traversal.GremlinLang.AddStep("addV", args.ToArray());
            }
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> AddV(GValue<string> vertexLabel)
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("addV", vertexLabel);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> AddV(ITraversal label, params ITraversal[] additionalLabels)
        {
            if (label == null) throw new ArgumentNullException(nameof(label));
            if (additionalLabels == null) throw new ArgumentNullException(nameof(additionalLabels));

            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, GremlinLang.Clone());
            if (additionalLabels.Length == 0)
            {
                traversal.GremlinLang.AddStep("addV", label);
            }
            else
            {
                var args = new List<object>(1 + additionalLabels.Length) { label };
                args.AddRange(additionalLabels);
                traversal.GremlinLang.AddStep("addV", args.ToArray());
            }
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the mergeV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> MergeV(IDictionary<object,object>? m)
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("mergeV", m);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the mergeV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> MergeV(GValue<IDictionary<object, object>> searchCreate)
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("mergeV", searchCreate);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the mergeV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> MergeV(ITraversal? t)
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("mergeV", t);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the inject step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Inject<TStart>(params TStart?[]? starts)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, GremlinLang.Clone());

            // null starts is treated as g.inject(null) meaning inject a single null traverser
            if (starts == null)
            {
                traversal.GremlinLang.AddStep("inject", new object?[] { null });
            }
            else
            {
                var args = new List<object?>(starts.Length);
                args.AddRange(starts.Cast<object?>());
                traversal.GremlinLang.AddStep("inject", args.ToArray());
            }

            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the io step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Io<TStart>(string file)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("io", file);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>()
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("call");
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>(string? service)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("call", service);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>(string? service, IDictionary<object, object>? m)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("call", service, m);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>(string? service, ITraversal? t)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("call", service, t);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>(string? service, IDictionary<object, object>? m,
            ITraversal? t)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("call", service, m, t);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>(string service, GValue<IDictionary<object, object>> m)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("call", service, m);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>(string service, GValue<IDictionary<object, object>> m,
            ITraversal childTraversal)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("call", service, m, childTraversal);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the union step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Union<TStart>(params ITraversal[] unionTraversals)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, GremlinLang.Clone());
            var args = new List<object>(unionTraversals.Length);
            args.AddRange(unionTraversals);
            traversal.GremlinLang.AddStep("union", args.ToArray());
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and executes a declarative
        ///     pattern match query. The step requires a graph provider to register an execution strategy before the
        ///     traversal can be executed.
        /// </summary>
        /// <param name="matchQuery">The declarative query string.</param>
        public GraphTraversal<object, object> Match(string matchQuery)
        {
            var traversal = new GraphTraversal<object, object>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("match", matchQuery);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and executes a declarative
        ///     pattern match query with bound parameters. The step requires a graph provider to register an execution
        ///     strategy before the traversal can be executed.
        /// </summary>
        /// <param name="matchQuery">The declarative query string.</param>
        /// <param name="parameters">The query parameters.</param>
        public GraphTraversal<object, object> Match(string matchQuery, IDictionary<object, object> parameters)
        {
            var traversal = new GraphTraversal<object, object>(TraversalStrategies, GremlinLang.Clone());
            traversal.GremlinLang.AddStep("match", matchQuery, parameters);
            return traversal;
        }

    }
    
#pragma warning restore 1591
}