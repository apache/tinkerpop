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
using Gremlin.Net.Structure;

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     Graph traversals are the primary way in which graphs are processed.
    /// </summary>
    public class GraphTraversal<TStart, TEnd> : DefaultTraversal<TStart, TEnd>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversal{SType, EType}" /> class.
        /// </summary>
        public GraphTraversal()
            : this(new List<ITraversalStrategy>(), new Bytecode(), true)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversal{SType, EType}" /> class.
        /// </summary>
        /// <param name="traversalStrategies">The traversal strategies to be used by this graph traversal at evaluation time.</param>
        /// <param name="bytecode">The <see cref="Bytecode" /> associated with the construction of this graph traversal.</param>
        public GraphTraversal(ICollection<ITraversalStrategy> traversalStrategies, Bytecode bytecode)
            : this(traversalStrategies, bytecode, false)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversal{SType, EType}" /> class.
        /// </summary>
        /// <param name="traversalStrategies">The traversal strategies to be used by this graph traversal at evaluation time.</param>
        /// <param name="bytecode">The <see cref="Bytecode" /> associated with the construction of this graph traversal.</param>
        /// <param name="anonymous">Set to true if spawned from an anonymous traversal source and false otherwise.</param>
        private GraphTraversal(ICollection<ITraversalStrategy> traversalStrategies, Bytecode bytecode, bool anonymous)
        {
            TraversalStrategies = traversalStrategies;
            Bytecode = bytecode;
            IsAnonymous = anonymous;
        }

        /// <inheritdoc />
        public override Bytecode Bytecode { get; }

        private static GraphTraversal<TNewStart, TNewEnd> Wrap<TNewStart, TNewEnd>(GraphTraversal<TStart, TEnd> traversal)
        {
            if (typeof(TNewStart) == typeof(TStart) && typeof(TNewEnd) == typeof(TEnd))
            {
                return (traversal as GraphTraversal<TNewStart, TNewEnd>)!;
            }
            // New wrapper
            return new GraphTraversal<TNewStart, TNewEnd>(traversal.TraversalStrategies, traversal.Bytecode, traversal.IsAnonymous);
        }


        /// <summary>
        ///     Adds the V step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> V (params object?[]? vertexIdsOrElements)
        {
            if (vertexIdsOrElements == null)
            {
                Bytecode.AddStep("V", new object?[] { null });
            }
            else
            {
                var args = new List<object?>(vertexIdsOrElements.Length);
                args.AddRange(vertexIdsOrElements);
                Bytecode.AddStep("V", args.ToArray());
            }
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the E step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> E (params object?[]? edgeIdsOrElements)
        {
            if (edgeIdsOrElements == null)
            {
                Bytecode.AddStep("E", new object?[] { null });
            }
            else
            {
                var args = new List<object?>(edgeIdsOrElements.Length);
                args.AddRange(edgeIdsOrElements);
                Bytecode.AddStep("E", args.ToArray());
            }
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the addE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> AddE (string edgeLabel)
        {
            Bytecode.AddStep("addE", edgeLabel);
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the addE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> AddE (ITraversal edgeLabelTraversal)
        {
            Bytecode.AddStep("addE", edgeLabelTraversal);
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the addV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> AddV ()
        {
            Bytecode.AddStep("addV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the addV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> AddV (string vertexLabel)
        {
            Bytecode.AddStep("addV", vertexLabel);
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the addV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> AddV (ITraversal vertexLabelTraversal)
        {
            Bytecode.AddStep("addV", vertexLabelTraversal);
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the aggregate step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Aggregate (Scope scope, string sideEffectKey)
        {
            Bytecode.AddStep("aggregate", scope, sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the aggregate step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Aggregate (string sideEffectKey)
        {
            Bytecode.AddStep("aggregate", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the and step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> And (params ITraversal[] andTraversals)
        {
            if (andTraversals == null) throw new ArgumentNullException(nameof(andTraversals));
            
            var args = new List<object>(andTraversals.Length);
            args.AddRange(andTraversals);
            Bytecode.AddStep("and", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the as step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> As (string stepLabel, params string[] stepLabels)
        {
            if (stepLabel == null) throw new ArgumentNullException(nameof(stepLabel));
            if (stepLabels == null) throw new ArgumentNullException(nameof(stepLabels));
            
            var args = new List<object>(1 + stepLabels.Length) {stepLabel};
            args.AddRange(stepLabels);
            Bytecode.AddStep("as", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the barrier step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Barrier ()
        {
            Bytecode.AddStep("barrier");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the barrier step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Barrier (IConsumer barrierConsumer)
        {
            Bytecode.AddStep("barrier", barrierConsumer);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the barrier step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Barrier (int maxBarrierSize)
        {
            Bytecode.AddStep("barrier", maxBarrierSize);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the both step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> Both (params string?[] edgeLabels)
        {
            if (edgeLabels == null) throw new ArgumentNullException(nameof(edgeLabels));
            
            var args = new List<object?>(edgeLabels.Length);
            args.AddRange(edgeLabels);
            Bytecode.AddStep("both", args.ToArray());
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the bothE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> BothE (params string?[] edgeLabels)
        {
            if (edgeLabels == null) throw new ArgumentNullException(nameof(edgeLabels));
            
            var args = new List<object?>(edgeLabels.Length);
            args.AddRange(edgeLabels);
            Bytecode.AddStep("bothE", args.ToArray());
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the bothV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> BothV ()
        {
            Bytecode.AddStep("bothV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the branch step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Branch<TNewEnd> (IFunction? function)
        {
            Bytecode.AddStep("branch", function);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the branch step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Branch<TNewEnd> (ITraversal branchTraversal)
        {
            Bytecode.AddStep("branch", branchTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By ()
        {
            Bytecode.AddStep("by");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (IComparator comparator)
        {
            Bytecode.AddStep("by", comparator);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (IFunction function)
        {
            Bytecode.AddStep("by", function);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (IFunction function, IComparator comparator)
        {
            Bytecode.AddStep("by", function, comparator);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (Order order)
        {
            Bytecode.AddStep("by", order);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (string? key)
        {
            Bytecode.AddStep("by", key);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (string? key, IComparator comparator)
        {
            Bytecode.AddStep("by", key, comparator);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (T token)
        {
            Bytecode.AddStep("by", token);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (ITraversal traversal)
        {
            Bytecode.AddStep("by", traversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (ITraversal traversal, IComparator comparator)
        {
            Bytecode.AddStep("by", traversal, comparator);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the call step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Call<TNewEnd> (string? service)
        {
            Bytecode.AddStep("call", service);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the call step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Call<TNewEnd> (string? service, ITraversal? t)
        {
            Bytecode.AddStep("call", service, t);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the call step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Call<TNewEnd> (string? service, IDictionary<object,object>? m)
        {
            Bytecode.AddStep("call", service, m);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the call step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Call<TNewEnd>(string? service, IDictionary<object, object>? m,
            ITraversal? t)
        {
            Bytecode.AddStep("call", service, m, t);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the cap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Cap<TNewEnd> (string sideEffectKey, params string[] sideEffectKeys)
        {
            if (sideEffectKey == null) throw new ArgumentNullException(nameof(sideEffectKey));
            if (sideEffectKeys == null) throw new ArgumentNullException(nameof(sideEffectKeys));

            var args = new List<object>(1 + sideEffectKeys.Length) { sideEffectKey };
            args.AddRange(sideEffectKeys);
            Bytecode.AddStep("cap", args.ToArray());
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (IFunction choiceFunction)
        {
            Bytecode.AddStep("choose", choiceFunction);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (IPredicate choosePredicate, ITraversal trueChoice)
        {
            Bytecode.AddStep("choose", choosePredicate, trueChoice);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (IPredicate choosePredicate, ITraversal trueChoice, ITraversal falseChoice)
        {
            Bytecode.AddStep("choose", choosePredicate, trueChoice, falseChoice);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (ITraversal choiceTraversal)
        {
            Bytecode.AddStep("choose", choiceTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (ITraversal traversalPredicate, ITraversal trueChoice)
        {
            Bytecode.AddStep("choose", traversalPredicate, trueChoice);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (ITraversal traversalPredicate, ITraversal trueChoice, ITraversal falseChoice)
        {
            Bytecode.AddStep("choose", traversalPredicate, trueChoice, falseChoice);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the coalesce step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Coalesce<TNewEnd> (params ITraversal[] coalesceTraversals)
        {
            if (coalesceTraversals == null) throw new ArgumentNullException(nameof(coalesceTraversals));

            var args = new List<object>(coalesceTraversals.Length);
            args.AddRange(coalesceTraversals);
            Bytecode.AddStep("coalesce", args.ToArray());
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the coin step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Coin (double probability)
        {
            Bytecode.AddStep("coin", probability);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the connectedComponent step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> ConnectedComponent ()
        {
            Bytecode.AddStep("connectedComponent");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the constant step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Constant<TNewEnd> (TNewEnd e)
        {
            Bytecode.AddStep("constant", e);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the count step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, long> Count ()
        {
            Bytecode.AddStep("count");
            return Wrap<TStart, long>(this);
        }

        /// <summary>
        ///     Adds the count step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, long> Count (Scope scope)
        {
            Bytecode.AddStep("count", scope);
            return Wrap<TStart, long>(this);
        }

        /// <summary>
        ///     Adds the cyclicPath step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> CyclicPath ()
        {
            Bytecode.AddStep("cyclicPath");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the dedup step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Dedup (Scope scope, params string?[] dedupLabels)
        {
            if (dedupLabels == null) throw new ArgumentNullException(nameof(dedupLabels));

            var args = new List<object?>(1 + dedupLabels.Length) { scope };
            args.AddRange(dedupLabels);
            Bytecode.AddStep("dedup", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the dedup step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Dedup (params string?[] dedupLabels)
        {
            if (dedupLabels == null) throw new ArgumentNullException(nameof(dedupLabels));

            var args = new List<object?>(dedupLabels.Length);
            args.AddRange(dedupLabels);
            Bytecode.AddStep("dedup", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the drop step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Drop ()
        {
            Bytecode.AddStep("drop");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the element step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Element> Element ()
        {
            Bytecode.AddStep("element");
            return Wrap<TStart, Element>(this);
        }

        /// <summary>
        ///     Adds the elementMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<object, TNewEnd>> ElementMap<TNewEnd> (params string?[] propertyKeys)
        {
            if (propertyKeys == null) throw new ArgumentNullException(nameof(propertyKeys));
            
            var args = new List<object?>(0 + propertyKeys.Length);
            args.AddRange(propertyKeys);
            Bytecode.AddStep("elementMap", args.ToArray());
            return Wrap<TStart, IDictionary<object, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the emit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Emit ()
        {
            Bytecode.AddStep("emit");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the emit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Emit (IPredicate emitPredicate)
        {
            Bytecode.AddStep("emit", emitPredicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the emit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Emit (ITraversal emitTraversal)
        {
            Bytecode.AddStep("emit", emitTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the fail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Fail ()
        {
            Bytecode.AddStep("fail");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the fail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Fail (string? msg)
        {
            Bytecode.AddStep("fail", msg);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the fail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Fail (string? msg, IDictionary<string,object> m)
        {
            Bytecode.AddStep("fail", msg, m);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the filter step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Filter (IPredicate? predicate)
        {
            Bytecode.AddStep("filter", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the filter step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Filter (ITraversal filterTraversal)
        {
            Bytecode.AddStep("filter", filterTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the flatMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> FlatMap<TNewEnd> (IFunction? function)
        {
            Bytecode.AddStep("flatMap", function);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the flatMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> FlatMap<TNewEnd> (ITraversal flatMapTraversal)
        {
            Bytecode.AddStep("flatMap", flatMapTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the fold step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IList<TEnd>> Fold ()
        {
            Bytecode.AddStep("fold");
            return Wrap<TStart, IList<TEnd>>(this);
        }

        /// <summary>
        ///     Adds the fold step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Fold<TNewEnd> (TNewEnd seed, IBiFunction? foldFunction)
        {
            Bytecode.AddStep("fold", seed, foldFunction);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the from step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> From (string? fromStepLabel)
        {
            Bytecode.AddStep("from", fromStepLabel);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the from step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> From (ITraversal fromVertex)
        {
            Bytecode.AddStep("from", fromVertex);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the from step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> From (Vertex? fromVertex)
        {
            Bytecode.AddStep("from", fromVertex);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the group step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<K, V>> Group<K, V> ()
        {
            Bytecode.AddStep("group");
            return Wrap<TStart, IDictionary<K, V>>(this);
        }

        /// <summary>
        ///     Adds the group step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Group (string sideEffectKey)
        {
            Bytecode.AddStep("group", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the groupCount step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<K, long>> GroupCount<K> ()
        {
            Bytecode.AddStep("groupCount");
            return Wrap<TStart, IDictionary<K, long>>(this);
        }

        /// <summary>
        ///     Adds the groupCount step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> GroupCount (string sideEffectKey)
        {
            Bytecode.AddStep("groupCount", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? propertyKey)
        {
            Bytecode.AddStep("has", propertyKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? propertyKey, object? value)
        {
            Bytecode.AddStep("has", propertyKey, value);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? propertyKey, P? predicate)
        {
            Bytecode.AddStep("has", propertyKey, predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? label, string? propertyKey, object? value)
        {
            Bytecode.AddStep("has", label, propertyKey, value);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? label, string? propertyKey, P? predicate)
        {
            Bytecode.AddStep("has", label, propertyKey, predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? propertyKey, ITraversal propertyTraversal)
        {
            Bytecode.AddStep("has", propertyKey, propertyTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (T accessor, object? value)
        {
            Bytecode.AddStep("has", accessor, value);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (T accessor, P? predicate)
        {
            Bytecode.AddStep("has", accessor, predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (T accessor, ITraversal propertyTraversal)
        {
            Bytecode.AddStep("has", accessor, propertyTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasId step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasId (object? id, params object?[]? otherIds)
        {
            List<object?> args;
            if (otherIds == null)
            {
                args = new List<object?> { id, null };
            }
            else
            {
                args = new List<object?>(1 + otherIds.Length) { id };
                args.AddRange(otherIds);
            }
            Bytecode.AddStep("hasId", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasId step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasId (P? predicate)
        {
            Bytecode.AddStep("hasId", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasKey step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasKey (P? predicate)
        {
            Bytecode.AddStep("hasKey", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasKey step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasKey (string? label, params string?[]? otherLabels)
        {
            List<object?> args;
            if (otherLabels == null)
            {
                args = new List<object?> { label, null };
            }
            else
            {
                args = new List<object?>(1 + otherLabels.Length) { label };
                args.AddRange(otherLabels);
            }
            Bytecode.AddStep("hasKey", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasLabel step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasLabel (P? predicate)
        {
            Bytecode.AddStep("hasLabel", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasLabel step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasLabel (string? label, params string?[]? otherLabels)
        {
            List<object?> args;
            if (otherLabels == null)
            {
                args = new List<object?> { label, null };
            }
            else
            {
                args = new List<object?>(1 + otherLabels.Length) { label };
                args.AddRange(otherLabels);
            }
            Bytecode.AddStep("hasLabel", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasNot step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasNot (string? propertyKey)
        {
            Bytecode.AddStep("hasNot", propertyKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasValue step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasValue (object? value, params object?[]? otherValues)
        {
            List<object?> args;
            if (otherValues == null)
            {
                args = new List<object?> { value, null };
            }
            else
            {
                args = new List<object?>(1 + otherValues.Length) { value };
                args.AddRange(otherValues);
            }
            Bytecode.AddStep("hasValue", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasValue step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasValue (P? predicate)
        {
            Bytecode.AddStep("hasValue", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the id step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, object> Id ()
        {
            Bytecode.AddStep("id");
            return Wrap<TStart, object>(this);
        }

        /// <summary>
        ///     Adds the identity step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Identity ()
        {
            Bytecode.AddStep("identity");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the in step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> In (params string?[] edgeLabels)
        {
            if (edgeLabels == null) throw new ArgumentNullException(nameof(edgeLabels));
            
            var args = new List<object?>(edgeLabels.Length);
            args.AddRange(edgeLabels);
            Bytecode.AddStep("in", args.ToArray());
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the inE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> InE (params string?[] edgeLabels)
        {
            if (edgeLabels == null) throw new ArgumentNullException(nameof(edgeLabels));
            
            var args = new List<object?>(edgeLabels.Length);
            args.AddRange(edgeLabels);
            Bytecode.AddStep("inE", args.ToArray());
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the inV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> InV ()
        {
            Bytecode.AddStep("inV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the index step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Index<TNewEnd> ()
        {
            Bytecode.AddStep("index");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the inject step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Inject (params TEnd?[]? injections)
        {
            // null injections is treated as g.inject(null) meaning inject a single null traverser
            if (injections == null)
            {
                Bytecode.AddStep("inject", new object?[] { null });
            }
            else
            {
                var args = new List<object?>(injections.Length);
                args.AddRange(injections.Cast<object?>());
                Bytecode.AddStep("inject", args.ToArray());
            }

            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the is step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Is (object? value)
        {
            Bytecode.AddStep("is", value);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the is step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Is (P? predicate)
        {
            Bytecode.AddStep("is", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the key step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string> Key ()
        {
            Bytecode.AddStep("key");
            return Wrap<TStart, string>(this);
        }

        /// <summary>
        ///     Adds the label step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string> Label ()
        {
            Bytecode.AddStep("label");
            return Wrap<TStart, string>(this);
        }

        /// <summary>
        ///     Adds the limit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Limit<TNewEnd> (Scope scope, long limit)
        {
            Bytecode.AddStep("limit", scope, limit);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the limit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Limit<TNewEnd> (long limit)
        {
            Bytecode.AddStep("limit", limit);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the local step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Local<TNewEnd> (ITraversal localTraversal)
        {
            Bytecode.AddStep("local", localTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the loops step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, int> Loops ()
        {
            Bytecode.AddStep("loops");
            return Wrap<TStart, int>(this);
        }

        /// <summary>
        ///     Adds the loops step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, int> Loops (string? loopName)
        {
            Bytecode.AddStep("loops", loopName);
            return Wrap<TStart, int>(this);
        }

        /// <summary>
        ///     Adds the map step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Map<TNewEnd> (IFunction? function)
        {
            Bytecode.AddStep("map", function);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the map step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Map<TNewEnd> (ITraversal mapTraversal)
        {
            Bytecode.AddStep("map", mapTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the match step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<string, TNewEnd>> Match<TNewEnd> (params ITraversal[] matchTraversals)
        {
            if (matchTraversals == null) throw new ArgumentNullException(nameof(matchTraversals));

            var args = new List<object>(matchTraversals.Length);
            args.AddRange(matchTraversals);
            Bytecode.AddStep("match", args.ToArray());
            return Wrap<TStart, IDictionary<string, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the math step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, double> Math (string expression)
        {
            Bytecode.AddStep("math", expression);
            return Wrap<TStart, double>(this);
        }

        /// <summary>
        ///     Adds the max step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Max<TNewEnd> ()
        {
            Bytecode.AddStep("max");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the max step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Max<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("max", scope);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the mean step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Mean<TNewEnd> ()
        {
            Bytecode.AddStep("mean");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the mean step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Mean<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("mean", scope);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the mergeE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> MergeE ()
        {
            Bytecode.AddStep("mergeE");
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the mergeE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> MergeE (IDictionary<object,object>? m)
        {
            Bytecode.AddStep("mergeE", m);
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the mergeE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> MergeE (ITraversal? t)
        {
            Bytecode.AddStep("mergeE", t);
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the mergeV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> MergeV ()
        {
            Bytecode.AddStep("mergeV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the mergeV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> MergeV (IDictionary<object,object>? m)
        {
            Bytecode.AddStep("mergeV", m);
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the mergeV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> MergeV (ITraversal? t)
        {
            Bytecode.AddStep("mergeV", t);
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the min step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Min<TNewEnd> ()
        {
            Bytecode.AddStep("min");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the min step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Min<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("min", scope);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the none step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> None ()
        {
            Bytecode.AddStep("none");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the not step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Not (ITraversal notTraversal)
        {
            Bytecode.AddStep("not", notTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the option step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Option (object pickToken, ITraversal? traversalOption)
        {
            Bytecode.AddStep("option", pickToken, traversalOption);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the option step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Option (object pickToken, IDictionary<object,object>? traversalOption)
        {
            Bytecode.AddStep("option", pickToken, traversalOption);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the option step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Option (object pickToken, IDictionary<object,object> traversalOption, Cardinality cardinality)
        {
            Bytecode.AddStep("option", pickToken, traversalOption, cardinality);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the option step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Option (ITraversal? traversalOption)
        {
            Bytecode.AddStep("option", traversalOption);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the optional step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Optional<TNewEnd> (ITraversal optionalTraversal)
        {
            Bytecode.AddStep("optional", optionalTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the or step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Or (params ITraversal[] orTraversals)
        {
            if (orTraversals == null) throw new ArgumentNullException(nameof(orTraversals));

            var args = new List<object>(orTraversals.Length);
            args.AddRange(orTraversals);
            Bytecode.AddStep("or", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the order step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Order ()
        {
            Bytecode.AddStep("order");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the order step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Order (Scope scope)
        {
            Bytecode.AddStep("order", scope);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the otherV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> OtherV ()
        {
            Bytecode.AddStep("otherV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the out step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> Out (params string?[] edgeLabels)
        {
            if (edgeLabels == null) throw new ArgumentNullException(nameof(edgeLabels));
            
            var args = new List<object?>(edgeLabels.Length);
            args.AddRange(edgeLabels);
            Bytecode.AddStep("out", args.ToArray());
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the outE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> OutE (params string?[] edgeLabels)
        {
            if (edgeLabels == null) throw new ArgumentNullException(nameof(edgeLabels));
            
            var args = new List<object?>(edgeLabels.Length);
            args.AddRange(edgeLabels);
            Bytecode.AddStep("outE", args.ToArray());
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the outV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> OutV ()
        {
            Bytecode.AddStep("outV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the pageRank step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> PageRank ()
        {
            Bytecode.AddStep("pageRank");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the pageRank step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> PageRank (double alpha)
        {
            Bytecode.AddStep("pageRank", alpha);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the path step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Path> Path ()
        {
            Bytecode.AddStep("path");
            return Wrap<TStart, Path>(this);
        }

        /// <summary>
        ///     Adds the peerPressure step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> PeerPressure ()
        {
            Bytecode.AddStep("peerPressure");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the profile step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Profile<TNewEnd> ()
        {
            Bytecode.AddStep("profile");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the profile step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Profile (string sideEffectKey)
        {
            Bytecode.AddStep("profile", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the program step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Program (object vertexProgram)
        {
            Bytecode.AddStep("program", vertexProgram);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the project step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<string, TNewEnd>> Project<TNewEnd>(string? projectKey,
            params string?[] otherProjectKeys)
        {
            // Using null as a key is allowed in Java, but we cannot support it in .NET as null is not allowed as a
            //  Dictionary key.
            if (projectKey == null) throw new ArgumentNullException(nameof(projectKey));
            if (otherProjectKeys == null) throw new ArgumentNullException(nameof(otherProjectKeys));
            
            var args = new List<object?>(1 + otherProjectKeys.Length) { projectKey };
            args.AddRange(otherProjectKeys);
            Bytecode.AddStep("project", args.ToArray());
            return Wrap<TStart, IDictionary<string, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the properties step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Properties<TNewEnd> (params string?[] propertyKeys)
        {
            // Using null as a key is allowed in Java, but we cannot support it in .NET as null is not allowed as a
            //  Dictionary key.
            if (propertyKeys == null) throw new ArgumentNullException(nameof(propertyKeys));
            
            var args = new List<object?>(propertyKeys.Length);
            args.AddRange(propertyKeys);
            Bytecode.AddStep("properties", args.ToArray());
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the property step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Property(Cardinality cardinality, object? key, object? value,
            params object?[] keyValues)
        {
            if (keyValues == null) throw new ArgumentNullException(nameof(keyValues));

            var args = new List<object?>(3 + keyValues.Length) { cardinality, key, value };
            args.AddRange(keyValues);
            Bytecode.AddStep("property", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the property step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Property (object? key, object? value, params object?[] keyValues)
        {
            if (keyValues == null) throw new ArgumentNullException(nameof(keyValues));

            var args = new List<object?>(2 + keyValues.Length) { key, value };
            args.AddRange(keyValues);
            Bytecode.AddStep("property", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the propertyMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<string, TNewEnd>> PropertyMap<TNewEnd> (params string?[] propertyKeys)
        {
            if (propertyKeys == null) throw new ArgumentNullException(nameof(propertyKeys));
            
            var args = new List<object?>(propertyKeys.Length);
            args.AddRange(propertyKeys);
            Bytecode.AddStep("propertyMap", args.ToArray());
            return Wrap<TStart, IDictionary<string, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the range step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Range<TNewEnd> (Scope scope, long low, long high)
        {
            Bytecode.AddStep("range", scope, low, high);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the range step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Range<TNewEnd> (long low, long high)
        {
            Bytecode.AddStep("range", low, high);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the read step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Read ()
        {
            Bytecode.AddStep("read");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the repeat step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Repeat (string loopName, ITraversal repeatTraversal)
        {
            Bytecode.AddStep("repeat", loopName, repeatTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the repeat step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Repeat (ITraversal repeatTraversal)
        {
            Bytecode.AddStep("repeat", repeatTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the sack step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Sack<TNewEnd> ()
        {
            Bytecode.AddStep("sack");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the sack step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Sack (IBiFunction sackOperator)
        {
            Bytecode.AddStep("sack", sackOperator);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the sample step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Sample (Scope scope, int amountToSample)
        {
            Bytecode.AddStep("sample", scope, amountToSample);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the sample step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Sample (int amountToSample)
        {
            Bytecode.AddStep("sample", amountToSample);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, ICollection<TNewEnd>> Select<TNewEnd> (Column column)
        {
            Bytecode.AddStep("select", column);
            return Wrap<TStart, ICollection<TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Select<TNewEnd> (Pop pop, string? selectKey)
        {
            Bytecode.AddStep("select", pop, selectKey);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<string, TNewEnd>> Select<TNewEnd>(Pop pop, string? selectKey1,
            string? selectKey2, params string?[] otherSelectKeys)
        {
            if (otherSelectKeys == null) throw new ArgumentNullException(nameof(otherSelectKeys));

            var args = new List<object?>(3 + otherSelectKeys.Length) { pop, selectKey1, selectKey2 };
            args.AddRange(otherSelectKeys);
            Bytecode.AddStep("select", args.ToArray());
            return Wrap<TStart, IDictionary<string, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Select<TNewEnd> (Pop pop, ITraversal keyTraversal)
        {
            Bytecode.AddStep("select", pop, keyTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Select<TNewEnd> (string? selectKey)
        {
            Bytecode.AddStep("select", selectKey);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<string, TNewEnd>> Select<TNewEnd>(string? selectKey1,
            string? selectKey2, params string?[] otherSelectKeys)
        {
            if (otherSelectKeys == null) throw new ArgumentNullException(nameof(otherSelectKeys));

            var args = new List<object?>(2 + otherSelectKeys.Length) { selectKey1, selectKey2 };
            args.AddRange(otherSelectKeys);
            Bytecode.AddStep("select", args.ToArray());
            return Wrap<TStart, IDictionary<string, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Select<TNewEnd> (ITraversal keyTraversal)
        {
            Bytecode.AddStep("select", keyTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the shortestPath step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Path> ShortestPath ()
        {
            Bytecode.AddStep("shortestPath");
            return Wrap<TStart, Path>(this);
        }

        /// <summary>
        ///     Adds the sideEffect step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> SideEffect (IConsumer? consumer)
        {
            Bytecode.AddStep("sideEffect", consumer);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the sideEffect step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> SideEffect (ITraversal sideEffectTraversal)
        {
            Bytecode.AddStep("sideEffect", sideEffectTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the simplePath step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> SimplePath ()
        {
            Bytecode.AddStep("simplePath");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the skip step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Skip<TNewEnd> (Scope scope, long skip)
        {
            Bytecode.AddStep("skip", scope, skip);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the skip step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Skip<TNewEnd> (long skip)
        {
            Bytecode.AddStep("skip", skip);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the store step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Store (string sideEffectKey)
        {
            Bytecode.AddStep("store", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the subgraph step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> Subgraph (string sideEffectKey)
        {
            Bytecode.AddStep("subgraph", sideEffectKey);
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the sum step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Sum<TNewEnd> ()
        {
            Bytecode.AddStep("sum");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the sum step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Sum<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("sum", scope);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the tail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Tail<TNewEnd> ()
        {
            Bytecode.AddStep("tail");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the tail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Tail<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("tail", scope);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the tail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Tail<TNewEnd> (Scope scope, long limit)
        {
            Bytecode.AddStep("tail", scope, limit);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the tail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Tail<TNewEnd> (long limit)
        {
            Bytecode.AddStep("tail", limit);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the timeLimit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> TimeLimit (long timeLimit)
        {
            Bytecode.AddStep("timeLimit", timeLimit);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the times step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Times (int maxLoops)
        {
            Bytecode.AddStep("times", maxLoops);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the to step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> To (Direction? direction, params string?[] edgeLabels)
        {
            if (edgeLabels == null) throw new ArgumentNullException(nameof(edgeLabels));

            var args = new List<object?>(1 + edgeLabels.Length) { direction };
            args.AddRange(edgeLabels);
            Bytecode.AddStep("to", args.ToArray());
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the to step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> To (string? toStepLabel)
        {
            Bytecode.AddStep("to", toStepLabel);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the to step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> To (ITraversal toVertex)
        {
            Bytecode.AddStep("to", toVertex);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the to step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> To (Vertex? toVertex)
        {
            Bytecode.AddStep("to", toVertex);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the toE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> ToE (Direction? direction, params string?[] edgeLabels)
        {
            if (edgeLabels == null) throw new ArgumentNullException(nameof(edgeLabels));
            
            var args = new List<object?>(1 + edgeLabels.Length) {direction};
            args.AddRange(edgeLabels);
            Bytecode.AddStep("toE", args.ToArray());
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the toV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> ToV (Direction? direction)
        {
            Bytecode.AddStep("toV", direction);
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the tree step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Tree<TNewEnd> ()
        {
            Bytecode.AddStep("tree");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the tree step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Tree (string sideEffectKey)
        {
            Bytecode.AddStep("tree", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the unfold step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Unfold<TNewEnd> ()
        {
            Bytecode.AddStep("unfold");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the union step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Union<TNewEnd> (params ITraversal[] unionTraversals)
        {
            if (unionTraversals == null) throw new ArgumentNullException(nameof(unionTraversals));
            
            var args = new List<object>(unionTraversals.Length);
            args.AddRange(unionTraversals);
            Bytecode.AddStep("union", args.ToArray());
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the until step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Until (IPredicate untilPredicate)
        {
            Bytecode.AddStep("until", untilPredicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the until step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Until (ITraversal untilTraversal)
        {
            Bytecode.AddStep("until", untilTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the value step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Value<TNewEnd> ()
        {
            Bytecode.AddStep("value");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the valueMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<TKey, TValue>> ValueMap<TKey, TValue> (params string?[] propertyKeys)
        {
            if (propertyKeys == null) throw new ArgumentNullException(nameof(propertyKeys));
            
            var args = new List<object?>(propertyKeys.Length);
            args.AddRange(propertyKeys);
            Bytecode.AddStep("valueMap", args.ToArray());
            return Wrap<TStart, IDictionary<TKey, TValue>>(this);
        }

        /// <summary>
        ///     Adds the valueMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<TKey, TValue>> ValueMap<TKey, TValue>(bool includeTokens,
            params string?[] propertyKeys)
        {
            if (propertyKeys == null) throw new ArgumentNullException(nameof(propertyKeys));

            var args = new List<object?>(1 + propertyKeys.Length) { includeTokens };
            args.AddRange(propertyKeys);
            Bytecode.AddStep("valueMap", args.ToArray());
            return Wrap<TStart, IDictionary<TKey, TValue>>(this);
        }

        /// <summary>
        ///     Adds the values step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Values<TNewEnd> (params string?[] propertyKeys)
        {
            if (propertyKeys == null) throw new ArgumentNullException(nameof(propertyKeys));
            
            var args = new List<object?>(propertyKeys.Length);
            args.AddRange(propertyKeys);
            Bytecode.AddStep("values", args.ToArray());
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the where step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Where (P predicate)
        {
            Bytecode.AddStep("where", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the where step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Where (string startKey, P predicate)
        {
            Bytecode.AddStep("where", startKey, predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the where step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Where (ITraversal whereTraversal)
        {
            Bytecode.AddStep("where", whereTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the with step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> With (string key)
        {
            Bytecode.AddStep("with", key);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the with step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> With (string key, object? value)
        {
            Bytecode.AddStep("with", key, value);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the write step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Write ()
        {
            Bytecode.AddStep("write");
            return Wrap<TStart, TEnd>(this);
        }


        /// <summary>
        /// Make a copy of a traversal that is reset for iteration.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Clone()
        {
            return new GraphTraversal<TStart, TEnd>(TraversalStrategies, new Bytecode(Bytecode));
        }
    }
}