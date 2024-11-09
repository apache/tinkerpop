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
using System.Linq;
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

        public bool IsSessionBound => _connection is { IsSessionBound: true };
        
        /// <summary>
        ///     Gets or sets the traversal strategies associated with this graph traversal source.
        /// </summary>
        public ICollection<ITraversalStrategy> TraversalStrategies { get; set; }

        /// <summary>
        ///     Gets or sets the <see cref="Traversal.Bytecode" /> associated with the current state of this graph traversal
        ///     source.
        /// </summary>
        public Bytecode Bytecode { get; set; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversalSource" /> class.
        /// </summary>
        public GraphTraversalSource()
            : this(new List<ITraversalStrategy>(), new Bytecode())
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversalSource" /> class.
        /// </summary>
        /// <param name="traversalStrategies">The traversal strategies associated with this graph traversal source.</param>
        /// <param name="bytecode">
        ///     The <see cref="Traversal.Bytecode" /> associated with the current state of this graph traversal
        ///     source.
        /// </param>
        public GraphTraversalSource(ICollection<ITraversalStrategy> traversalStrategies, Bytecode bytecode)
        {
            TraversalStrategies = traversalStrategies;
            Bytecode = bytecode;
        }

        public GraphTraversalSource(ICollection<ITraversalStrategy> traversalStrategies, Bytecode bytecode,
            IRemoteConnection connection)
            : this(traversalStrategies.Where(strategy => strategy.GetType() != typeof(RemoteStrategy)).ToList(),
                bytecode)
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
            var optionsStrategyInst = Bytecode.SourceInstructions.Find(
                inst => inst.OperatorName == "withStrategies" && inst.Arguments[0] is OptionsStrategy);
            OptionsStrategy optionsStrategy;

            if (optionsStrategyInst == null)
            {
                optionsStrategy = new OptionsStrategy();
                optionsStrategy.Configuration[key] = value;
                return WithStrategies(optionsStrategy);
            }

            optionsStrategy = optionsStrategyInst.Arguments[0]!;
            optionsStrategy.Configuration[key] = value;
            return new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                new Bytecode(Bytecode));
        }


        public GraphTraversalSource WithBulk(bool useBulk)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withBulk", useBulk);
            return source;
        }

        public GraphTraversalSource WithPath()
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withPath");
            return source;
        }

        public GraphTraversalSource WithSack(object? initialValue)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue);
            return source;
        }

        public GraphTraversalSource WithSack(object? initialValue, IBinaryOperator? mergeOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue, mergeOperator);
            return source;
        }

        public GraphTraversalSource WithSack(object? initialValue, IUnaryOperator? splitOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue, splitOperator);
            return source;
        }

        public GraphTraversalSource WithSack(object? initialValue, IUnaryOperator? splitOperator,
            IBinaryOperator? mergeOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue, splitOperator, mergeOperator);
            return source;
        }

        public GraphTraversalSource WithSack(ISupplier? initialValue)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue);
            return source;
        }

        public GraphTraversalSource WithSack(ISupplier? initialValue, IBinaryOperator? mergeOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue, mergeOperator);
            return source;
        }

        public GraphTraversalSource WithSack(ISupplier? initialValue, IUnaryOperator? splitOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue, splitOperator);
            return source;
        }

        public GraphTraversalSource WithSack(ISupplier? initialValue, IUnaryOperator? splitOperator,
            IBinaryOperator? mergeOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue, splitOperator, mergeOperator);
            return source;
        }

        public GraphTraversalSource WithSideEffect(string? key, object? initialValue)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSideEffect", key, initialValue);
            return source;
        }

        public GraphTraversalSource WithSideEffect(string? key, object? initialValue, IBinaryOperator reducer)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSideEffect", key, initialValue, reducer);
            return source;
        }

        public GraphTraversalSource WithSideEffect(string? key, ISupplier? initialValue)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSideEffect", key, initialValue);
            return source;
        }

        public GraphTraversalSource WithSideEffect(string? key, ISupplier? initialValue, IBinaryOperator? reducer)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSideEffect", key, initialValue, reducer);
            return source;
        }

        public GraphTraversalSource WithStrategies(params ITraversalStrategy[] traversalStrategies)
        {
            if (traversalStrategies == null) throw new ArgumentNullException(nameof(traversalStrategies));
            
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            var args = new List<object>(traversalStrategies.Length);
            args.AddRange(traversalStrategies);
            source.Bytecode.AddSource("withStrategies", args.ToArray());
            return source;
        }

        public GraphTraversalSource WithoutStrategies(params Type?[] traversalStrategyClasses)
        {
            if (traversalStrategyClasses == null) throw new ArgumentNullException(nameof(traversalStrategyClasses));
            
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            var args = new List<object?>(traversalStrategyClasses.Length);
            args.AddRange(traversalStrategyClasses);
            source.Bytecode.AddSource("withoutStrategies", args.ToArray());
            return source;
        }

        [Obsolete("Use the Bindings class instead.", false)]
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
        public GraphTraversalSource WithRemote(IRemoteConnection remoteConnection) =>
            new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                new Bytecode(Bytecode), remoteConnection);

        /// <summary>
        ///     Spawns a new <see cref="RemoteTransaction"/> object that can then start and stop a transaction.
        /// </summary>
        /// <returns>The spawned transaction.</returns>
        /// <exception cref="InvalidOperationException">Thrown if this traversal source is already bound to a session.</exception>
        public RemoteTransaction Tx()
        {
            // you can't do g.tx().begin().tx() - no child transactions
            if (IsSessionBound)
            {
                throw new InvalidOperationException(
                    "This GraphTraversalSource is already bound to a transaction - child transactions are not supported");
            }
            return _connection!.Tx(this);
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
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, new Bytecode(Bytecode));
            if (edgesIds == null)
            {
                traversal.Bytecode.AddStep("E", new object?[] { null });
            }
            else
            {
                var args = new List<object?>(edgesIds.Length);
                args.AddRange(edgesIds);
                traversal.Bytecode.AddStep("E", args.ToArray());
            }
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the V step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> V(params object?[]? vertexIds)
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, new Bytecode(Bytecode));
            if (vertexIds == null)
            {
                traversal.Bytecode.AddStep("V", new object?[] { null });
            }
            else
            {
                var args = new List<object?>(vertexIds.Length);
                args.AddRange(vertexIds);
                traversal.Bytecode.AddStep("V", args.ToArray());
            }
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addE step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> AddE(string label)
        {
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("addE", label);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addE step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> AddE(ITraversal edgeLabelTraversal)
        {
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("addE", edgeLabelTraversal);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the mergeE step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> MergeE(IDictionary<object,object>? m)
        {
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("mergeE", m);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the mergeE step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Edge, Edge> MergeE(ITraversal? t)
        {
            var traversal = new GraphTraversal<Edge, Edge>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("mergeE", t);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> AddV()
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("addV");
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> AddV(string label)
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("addV", label);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> AddV(ITraversal vertexLabelTraversal)
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("addV", vertexLabelTraversal);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the mergeV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> MergeV(IDictionary<object,object>? m)
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("mergeV", m);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the mergeV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<Vertex, Vertex> MergeV(ITraversal? t)
        {
            var traversal = new GraphTraversal<Vertex, Vertex>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("mergeV", t);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the inject step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Inject<TStart>(params TStart?[]? starts)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, new Bytecode(Bytecode));

            // null starts is treated as g.inject(null) meaning inject a single null traverser
            if (starts == null)
            {
                traversal.Bytecode.AddStep("inject", new object?[] { null });
            }
            else
            {
                var args = new List<object?>(starts.Length);
                args.AddRange(starts.Cast<object?>());
                traversal.Bytecode.AddStep("inject", args.ToArray());
            }

            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the io step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Io<TStart>(string file)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("io", file);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>()
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("call");
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>(string? service)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("call", service);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>(string? service, IDictionary<object, object>? m)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("call", service, m);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>(string? service, ITraversal? t)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("call", service, t);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the call step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Call<TStart>(string? service, IDictionary<object, object>? m,
            ITraversal? t)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("call", service, m, t);
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the union step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal<TStart, TStart> Union<TStart>(params ITraversal[] unionTraversals)
        {
            var traversal = new GraphTraversal<TStart, TStart>(TraversalStrategies, new Bytecode(Bytecode));
            var args = new List<object>(unionTraversals.Length);
            args.AddRange(unionTraversals);
            traversal.Bytecode.AddStep("union", args.ToArray());
            return traversal;
        }

    }
    
#pragma warning restore 1591
}