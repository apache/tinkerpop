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
using System.Linq;
using Gremlin.Net.Driver;
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
            : this(new List<ITraversalStrategy>(), new Bytecode(), new GremlinLang(), true)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversal{SType, EType}" /> class.
        /// </summary>
        /// <param name="traversalStrategies">The traversal strategies to be used by this graph traversal at evaluation time.</param>
        /// <param name="bytecode">The <see cref="Bytecode" /> associated with the construction of this graph traversal.</param>
        public GraphTraversal(ICollection<ITraversalStrategy> traversalStrategies, Bytecode bytecode)
            : this(traversalStrategies, bytecode, new GremlinLang(), false)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversal{SType, EType}" /> class.
        /// </summary>
        /// <param name="traversalStrategies">The traversal strategies to be used by this graph traversal at evaluation time.</param>
        /// <param name="bytecode">The <see cref="Bytecode" /> associated with the construction of this graph traversal.</param>
        /// <param name="gremlinLang">The <see cref="Traversal.GremlinLang" /> associated with the construction of this graph traversal.</param>
        public GraphTraversal(ICollection<ITraversalStrategy> traversalStrategies, Bytecode bytecode, GremlinLang gremlinLang)
            : this(traversalStrategies, bytecode, gremlinLang, false)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversal{SType, EType}" /> class.
        /// </summary>
        /// <param name="traversalStrategies">The traversal strategies to be used by this graph traversal at evaluation time.</param>
        /// <param name="bytecode">The <see cref="Bytecode" /> associated with the construction of this graph traversal.</param>
        /// <param name="gremlinLang">The <see cref="Traversal.GremlinLang" /> associated with the construction of this graph traversal.</param>
        /// <param name="anonymous">Set to true if spawned from an anonymous traversal source and false otherwise.</param>
        private GraphTraversal(ICollection<ITraversalStrategy> traversalStrategies, Bytecode bytecode, GremlinLang gremlinLang, bool anonymous)
        {
            TraversalStrategies = traversalStrategies;
            Bytecode = bytecode;
            GremlinLang = gremlinLang;
            IsAnonymous = anonymous;
        }

        /// <inheritdoc />
        public override Bytecode Bytecode { get; }

        /// <inheritdoc />
        public override GremlinLang GremlinLang { get; }

        private static GraphTraversal<TNewStart, TNewEnd> Wrap<TNewStart, TNewEnd>(GraphTraversal<TStart, TEnd> traversal)
        {
            if (typeof(TNewStart) == typeof(TStart) && typeof(TNewEnd) == typeof(TEnd))
            {
                return (traversal as GraphTraversal<TNewStart, TNewEnd>)!;
            }
            // New wrapper
            return new GraphTraversal<TNewStart, TNewEnd>(traversal.TraversalStrategies, traversal.Bytecode, traversal.GremlinLang, traversal.IsAnonymous);
        }


        /// <summary>
        ///     Adds the V step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> V (params object?[]? vertexIdsOrElements)
        {
            if (vertexIdsOrElements == null)
            {
                Bytecode.AddStep("V", new object?[] { null });

                GremlinLang.AddStep("V", new object?[] { null });
            }
            else
            {
                for (int i = 0; i < vertexIdsOrElements.Length; i++)
                {
                    if (vertexIdsOrElements[i] is Vertex vertex)
                    {
                        vertexIdsOrElements[i] = vertex.Id;
                    }
                }
                var args = new List<object?>(vertexIdsOrElements.Length);
                args.AddRange(vertexIdsOrElements);
                Bytecode.AddStep("V", args.ToArray());

                GremlinLang.AddStep("V", args.ToArray());
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

                GremlinLang.AddStep("E", new object?[] { null });
            }
            else
            {
                var args = new List<object?>(edgeIdsOrElements.Length);
                args.AddRange(edgeIdsOrElements);
                Bytecode.AddStep("E", args.ToArray());

                GremlinLang.AddStep("E", args.ToArray());
            }
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the addE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> AddE (string edgeLabel)
        {
            Bytecode.AddStep("addE", edgeLabel);

            GremlinLang.AddStep("addE", edgeLabel);
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the addE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> AddE (ITraversal edgeLabelTraversal)
        {
            Bytecode.AddStep("addE", edgeLabelTraversal);

            GremlinLang.AddStep("addE", edgeLabelTraversal);
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the addV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> AddV ()
        {
            Bytecode.AddStep("addV");

            GremlinLang.AddStep("addV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the addV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> AddV (string vertexLabel)
        {
            Bytecode.AddStep("addV", vertexLabel);

            GremlinLang.AddStep("addV", vertexLabel);
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the addV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> AddV (ITraversal vertexLabelTraversal)
        {
            Bytecode.AddStep("addV", vertexLabelTraversal);

            GremlinLang.AddStep("addV", vertexLabelTraversal);
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the aggregate step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Aggregate (string sideEffectKey)
        {
            Bytecode.AddStep("aggregate", sideEffectKey);

            GremlinLang.AddStep("aggregate", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the all step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> All (P? predicate)
        {
            Bytecode.AddStep("all", predicate);

            GremlinLang.AddStep("all", predicate);
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

            GremlinLang.AddStep("and", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the any step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Any (P? predicate)
        {
            Bytecode.AddStep("any", predicate);

            GremlinLang.AddStep("any", predicate);
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

            GremlinLang.AddStep("as", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }
        
        /// <summary>
        ///     Adds the asString step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string?> AsString ()
        {
            Bytecode.AddStep("asString");

            GremlinLang.AddStep("asString");
            return Wrap<TStart, string?>(this);
        }
        
        /// <summary>
        ///     Adds the asString step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd?> AsString<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("asString", scope);

            GremlinLang.AddStep("asString", scope);
            return Wrap<TStart, TNewEnd?>(this);
        }

        /// <summary>
        ///     Adds the asDate step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, DateTimeOffset> AsDate()
        {
            Bytecode.AddStep("asDate");

            GremlinLang.AddStep("asDate");
            return Wrap<TStart, DateTimeOffset>(this);
        }

        /// <summary>
        ///     Adds the asBool step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, bool> AsBool()
        {
            Bytecode.AddStep("asBool");

            GremlinLang.AddStep("asBool");
            return Wrap<TStart, bool>(this);
        }

        /// <summary>
        ///     Adds the barrier step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Barrier ()
        {
            Bytecode.AddStep("barrier");

            GremlinLang.AddStep("barrier");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the barrier step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Barrier (IConsumer barrierConsumer)
        {
            Bytecode.AddStep("barrier", barrierConsumer);

            GremlinLang.AddStep("barrier", barrierConsumer);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the barrier step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Barrier (int maxBarrierSize)
        {
            Bytecode.AddStep("barrier", maxBarrierSize);

            GremlinLang.AddStep("barrier", maxBarrierSize);
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

            GremlinLang.AddStep("both", args.ToArray());
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

            GremlinLang.AddStep("bothE", args.ToArray());
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the bothV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> BothV ()
        {
            Bytecode.AddStep("bothV");

            GremlinLang.AddStep("bothV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the branch step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Branch<TNewEnd> (IFunction? function)
        {
            Bytecode.AddStep("branch", function);

            GremlinLang.AddStep("branch", function);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the branch step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Branch<TNewEnd> (ITraversal branchTraversal)
        {
            Bytecode.AddStep("branch", branchTraversal);

            GremlinLang.AddStep("branch", branchTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By ()
        {
            Bytecode.AddStep("by");

            GremlinLang.AddStep("by");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (IComparator comparator)
        {
            Bytecode.AddStep("by", comparator);

            GremlinLang.AddStep("by", comparator);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (IFunction function)
        {
            Bytecode.AddStep("by", function);

            GremlinLang.AddStep("by", function);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (IFunction function, IComparator comparator)
        {
            Bytecode.AddStep("by", function, comparator);

            GremlinLang.AddStep("by", function, comparator);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (Order order)
        {
            Bytecode.AddStep("by", order);

            GremlinLang.AddStep("by", order);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (string? key)
        {
            Bytecode.AddStep("by", key);

            GremlinLang.AddStep("by", key);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (string? key, IComparator comparator)
        {
            Bytecode.AddStep("by", key, comparator);

            GremlinLang.AddStep("by", key, comparator);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (T token)
        {
            Bytecode.AddStep("by", token);

            GremlinLang.AddStep("by", token);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (ITraversal traversal)
        {
            Bytecode.AddStep("by", traversal);

            GremlinLang.AddStep("by", traversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> By (ITraversal traversal, IComparator comparator)
        {
            Bytecode.AddStep("by", traversal, comparator);

            GremlinLang.AddStep("by", traversal, comparator);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the call step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Call<TNewEnd> (string? service)
        {
            Bytecode.AddStep("call", service);

            GremlinLang.AddStep("call", service);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the call step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Call<TNewEnd> (string? service, ITraversal? t)
        {
            Bytecode.AddStep("call", service, t);

            GremlinLang.AddStep("call", service, t);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the call step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Call<TNewEnd> (string? service, IDictionary<object,object>? m)
        {
            Bytecode.AddStep("call", service, m);

            GremlinLang.AddStep("call", service, m);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the call step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Call<TNewEnd>(string? service, IDictionary<object, object>? m,
            ITraversal? t)
        {
            Bytecode.AddStep("call", service, m, t);

            GremlinLang.AddStep("call", service, m, t);
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

            GremlinLang.AddStep("cap", args.ToArray());
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (IFunction choiceFunction)
        {
            Bytecode.AddStep("choose", choiceFunction);

            GremlinLang.AddStep("choose", choiceFunction);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (IPredicate choosePredicate, ITraversal trueChoice)
        {
            Bytecode.AddStep("choose", choosePredicate, trueChoice);

            GremlinLang.AddStep("choose", choosePredicate, trueChoice);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (IPredicate choosePredicate, ITraversal trueChoice, ITraversal falseChoice)
        {
            Bytecode.AddStep("choose", choosePredicate, trueChoice, falseChoice);

            GremlinLang.AddStep("choose", choosePredicate, trueChoice, falseChoice);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (ITraversal choiceTraversal)
        {
            Bytecode.AddStep("choose", choiceTraversal);

            GremlinLang.AddStep("choose", choiceTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (ITraversal traversalPredicate, ITraversal trueChoice)
        {
            Bytecode.AddStep("choose", traversalPredicate, trueChoice);

            GremlinLang.AddStep("choose", traversalPredicate, trueChoice);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Choose<TNewEnd> (ITraversal traversalPredicate, ITraversal trueChoice, ITraversal falseChoice)
        {
            Bytecode.AddStep("choose", traversalPredicate, trueChoice, falseChoice);

            GremlinLang.AddStep("choose", traversalPredicate, trueChoice, falseChoice);
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

            GremlinLang.AddStep("coalesce", args.ToArray());
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the coin step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Coin (double probability)
        {
            Bytecode.AddStep("coin", probability);

            GremlinLang.AddStep("coin", probability);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the combine step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Combine (object combineObject)
        {
            Bytecode.AddStep("combine", combineObject);

            GremlinLang.AddStep("combine", combineObject);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the concat step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string> Concat (ITraversal concatTraversal, params ITraversal[]? otherConcatTraversals)
        {
            List<object?> args;
            if (otherConcatTraversals == null)
            {
                args = new List<object?> { concatTraversal };
            }
            else
            {
                args = new List<object?>(1 + otherConcatTraversals.Length) { concatTraversal };
                args.AddRange(otherConcatTraversals);
            }
            Bytecode.AddStep("concat", args.ToArray());

            GremlinLang.AddStep("concat", args.ToArray());
            return Wrap<TStart, string>(this);
        }

        /// <summary>
        ///     Adds the concat step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string> Concat (params string?[] concatStrings)
        {
            // need null check?

            var args = new List<object?>(concatStrings.Length);
            args.AddRange(concatStrings);
            Bytecode.AddStep("concat", args.ToArray());

            GremlinLang.AddStep("concat", args.ToArray());
            return Wrap<TStart, string>(this);
        }

        /// <summary>
        ///     Adds the conjoin step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd?> Conjoin (string delimiter)
        {
            Bytecode.AddStep("conjoin", delimiter);

            GremlinLang.AddStep("conjoin", delimiter);
            return Wrap<TStart, TEnd?>(this);
        }

        /// <summary>
        ///     Adds the connectedComponent step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> ConnectedComponent ()
        {
            Bytecode.AddStep("connectedComponent");

            GremlinLang.AddStep("connectedComponent");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the constant step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Constant<TNewEnd> (TNewEnd e)
        {
            Bytecode.AddStep("constant", e);

            GremlinLang.AddStep("constant", e);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the count step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, long> Count ()
        {
            Bytecode.AddStep("count");

            GremlinLang.AddStep("count");
            return Wrap<TStart, long>(this);
        }

        /// <summary>
        ///     Adds the count step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, long> Count (Scope scope)
        {
            Bytecode.AddStep("count", scope);

            GremlinLang.AddStep("count", scope);
            return Wrap<TStart, long>(this);
        }

        /// <summary>
        ///     Adds the cyclicPath step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> CyclicPath ()
        {
            Bytecode.AddStep("cyclicPath");

            GremlinLang.AddStep("cyclicPath");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the dateAdd step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, DateTimeOffset> DateAdd(DT dateToken, int value)
        {
            Bytecode.AddStep("dateAdd", dateToken, value);

            GremlinLang.AddStep("dateAdd", dateToken, value);
            return Wrap<TStart, DateTimeOffset>(this);
        }

        /// <summary>
        ///     Adds the dateDiff step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, long> DateDiff(DateTimeOffset value)
        {
            Bytecode.AddStep("dateDiff", value);

            GremlinLang.AddStep("dateDiff", value);
            return Wrap<TStart, long>(this);
        }

        /// <summary>
        ///     Adds the dateDiff step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, long> DateDiff(ITraversal dateTraversal)
        {
            Bytecode.AddStep("dateDiff", dateTraversal);

            GremlinLang.AddStep("dateDiff", dateTraversal);
            return Wrap<TStart, long>(this);
        }
        
        /// <summary>
        ///     Adds the asNumber step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, object> AsNumber()
        {
            Bytecode.AddStep("asNumber");

            GremlinLang.AddStep("asNumber");
            return Wrap<TStart, object>(this);
        }

        /// <summary>
        ///     Adds the asNumber step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, object> AsNumber(GType typeToken)
        {
            Bytecode.AddStep("asNumber", typeToken);

            GremlinLang.AddStep("asNumber", typeToken);
            return Wrap<TStart, object>(this);
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

            GremlinLang.AddStep("dedup", args.ToArray());
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

            GremlinLang.AddStep("dedup", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the difference step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Difference (object differenceObject)
        {
            Bytecode.AddStep("difference", differenceObject);

            GremlinLang.AddStep("difference", differenceObject);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the discard step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Discard ()
        {
            Bytecode.AddStep("discard");

            GremlinLang.AddStep("discard");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the disjunct step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Disjunct (object disjunctObject)
        {
            Bytecode.AddStep("disjunct", disjunctObject);

            GremlinLang.AddStep("disjunct", disjunctObject);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the drop step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Drop ()
        {
            Bytecode.AddStep("drop");

            GremlinLang.AddStep("drop");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the element step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Element> Element ()
        {
            Bytecode.AddStep("element");

            GremlinLang.AddStep("element");
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

            GremlinLang.AddStep("elementMap", args.ToArray());
            return Wrap<TStart, IDictionary<object, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the emit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Emit ()
        {
            Bytecode.AddStep("emit");

            GremlinLang.AddStep("emit");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the emit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Emit (IPredicate emitPredicate)
        {
            Bytecode.AddStep("emit", emitPredicate);

            GremlinLang.AddStep("emit", emitPredicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the emit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Emit (ITraversal emitTraversal)
        {
            Bytecode.AddStep("emit", emitTraversal);

            GremlinLang.AddStep("emit", emitTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the fail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Fail ()
        {
            Bytecode.AddStep("fail");

            GremlinLang.AddStep("fail");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the fail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Fail (string? msg)
        {
            Bytecode.AddStep("fail", msg);

            GremlinLang.AddStep("fail", msg);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the fail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Fail (string? msg, IDictionary<string,object> m)
        {
            Bytecode.AddStep("fail", msg, m);

            GremlinLang.AddStep("fail", msg, m);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the filter step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Filter (IPredicate? predicate)
        {
            Bytecode.AddStep("filter", predicate);

            GremlinLang.AddStep("filter", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the filter step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Filter (ITraversal filterTraversal)
        {
            Bytecode.AddStep("filter", filterTraversal);

            GremlinLang.AddStep("filter", filterTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the flatMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> FlatMap<TNewEnd> (IFunction? function)
        {
            Bytecode.AddStep("flatMap", function);

            GremlinLang.AddStep("flatMap", function);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the flatMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> FlatMap<TNewEnd> (ITraversal flatMapTraversal)
        {
            Bytecode.AddStep("flatMap", flatMapTraversal);

            GremlinLang.AddStep("flatMap", flatMapTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the fold step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IList<TEnd>> Fold ()
        {
            Bytecode.AddStep("fold");

            GremlinLang.AddStep("fold");
            return Wrap<TStart, IList<TEnd>>(this);
        }

        /// <summary>
        ///     Adds the fold step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Fold<TNewEnd> (TNewEnd seed, IBiFunction? foldFunction)
        {
            Bytecode.AddStep("fold", seed, foldFunction);

            GremlinLang.AddStep("fold", seed, foldFunction);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the format step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string> Format(string format)
        {
            Bytecode.AddStep("format", format);

            GremlinLang.AddStep("format", format);
            return Wrap<TStart, string>(this);
        }

        /// <summary>
        ///     Adds the from step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> From (object? fromStepVertexIdOrLabel)
        {
            var fromId = fromStepVertexIdOrLabel is Vertex fromVertex ? fromVertex.Id : fromStepVertexIdOrLabel;
            Bytecode.AddStep("from", fromId);
            GremlinLang.AddStep("from", fromId);
            return Wrap<TStart, TEnd>(this);
        }
        
        /// <summary>
        ///     Adds the from step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> From (string? fromStepLabel)
        {
            Bytecode.AddStep("from", fromStepLabel);

            GremlinLang.AddStep("from", fromStepLabel);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the from step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> From (ITraversal fromVertex)
        {
            Bytecode.AddStep("from", fromVertex);

            GremlinLang.AddStep("from", fromVertex);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the from step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> From (Vertex? fromVertex)
        {
            Bytecode.AddStep("from", fromVertex?.Id);

            GremlinLang.AddStep("from", fromVertex?.Id);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the group step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<K, V>> Group<K, V> ()
        {
            Bytecode.AddStep("group");

            GremlinLang.AddStep("group");
            return Wrap<TStart, IDictionary<K, V>>(this);
        }

        /// <summary>
        ///     Adds the group step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Group (string sideEffectKey)
        {
            Bytecode.AddStep("group", sideEffectKey);

            GremlinLang.AddStep("group", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the groupCount step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, IDictionary<K, long>> GroupCount<K> ()
        {
            Bytecode.AddStep("groupCount");

            GremlinLang.AddStep("groupCount");
            return Wrap<TStart, IDictionary<K, long>>(this);
        }

        /// <summary>
        ///     Adds the groupCount step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> GroupCount (string sideEffectKey)
        {
            Bytecode.AddStep("groupCount", sideEffectKey);

            GremlinLang.AddStep("groupCount", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? propertyKey)
        {
            Bytecode.AddStep("has", propertyKey);

            GremlinLang.AddStep("has", propertyKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? propertyKey, object? value)
        {
            Bytecode.AddStep("has", propertyKey, value);

            GremlinLang.AddStep("has", propertyKey, value);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? propertyKey, P? predicate)
        {
            Bytecode.AddStep("has", propertyKey, predicate);

            GremlinLang.AddStep("has", propertyKey, predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? label, string? propertyKey, object? value)
        {
            Bytecode.AddStep("has", label, propertyKey, value);

            GremlinLang.AddStep("has", label, propertyKey, value);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (string? label, string? propertyKey, P? predicate)
        {
            Bytecode.AddStep("has", label, propertyKey, predicate);

            GremlinLang.AddStep("has", label, propertyKey, predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (T accessor, object? value)
        {
            Bytecode.AddStep("has", accessor, value);

            GremlinLang.AddStep("has", accessor, value);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Has (T accessor, P? predicate)
        {
            Bytecode.AddStep("has", accessor, predicate);

            GremlinLang.AddStep("has", accessor, predicate);
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

            GremlinLang.AddStep("hasId", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasId step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasId (P? predicate)
        {
            Bytecode.AddStep("hasId", predicate);

            GremlinLang.AddStep("hasId", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasKey step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasKey (P? predicate)
        {
            Bytecode.AddStep("hasKey", predicate);

            GremlinLang.AddStep("hasKey", predicate);
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

            GremlinLang.AddStep("hasKey", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasLabel step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasLabel (P? predicate)
        {
            Bytecode.AddStep("hasLabel", predicate);

            GremlinLang.AddStep("hasLabel", predicate);
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

            GremlinLang.AddStep("hasLabel", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasNot step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasNot (string? propertyKey)
        {
            Bytecode.AddStep("hasNot", propertyKey);

            GremlinLang.AddStep("hasNot", propertyKey);
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

            GremlinLang.AddStep("hasValue", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the hasValue step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> HasValue (P? predicate)
        {
            Bytecode.AddStep("hasValue", predicate);

            GremlinLang.AddStep("hasValue", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the id step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, object> Id ()
        {
            Bytecode.AddStep("id");

            GremlinLang.AddStep("id");
            return Wrap<TStart, object>(this);
        }

        /// <summary>
        ///     Adds the identity step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Identity ()
        {
            Bytecode.AddStep("identity");

            GremlinLang.AddStep("identity");
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

            GremlinLang.AddStep("in", args.ToArray());
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

            GremlinLang.AddStep("inE", args.ToArray());
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the inV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> InV ()
        {
            Bytecode.AddStep("inV");

            GremlinLang.AddStep("inV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the index step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Index<TNewEnd> ()
        {
            Bytecode.AddStep("index");

            GremlinLang.AddStep("index");
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

                GremlinLang.AddStep("inject", new object?[] { null });
            }
            else
            {
                var args = new List<object?>(injections.Length);
                args.AddRange(injections.Cast<object?>());
                Bytecode.AddStep("inject", args.ToArray());

                GremlinLang.AddStep("inject", args.ToArray());
            }

            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the intersect step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Intersect (object intersectObject)
        {
            Bytecode.AddStep("intersect", intersectObject);

            GremlinLang.AddStep("intersect", intersectObject);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the is step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Is (object? value)
        {
            Bytecode.AddStep("is", value);

            GremlinLang.AddStep("is", value);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the is step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Is (P? predicate)
        {
            Bytecode.AddStep("is", predicate);

            GremlinLang.AddStep("is", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the key step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string> Key ()
        {
            Bytecode.AddStep("key");

            GremlinLang.AddStep("key");
            return Wrap<TStart, string>(this);
        }

        /// <summary>
        ///     Adds the label step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string> Label ()
        {
            Bytecode.AddStep("label");

            GremlinLang.AddStep("label");
            return Wrap<TStart, string>(this);
        }
        
        /// <summary>
        ///     Adds the length step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, int?> Length ()
        {
            Bytecode.AddStep("length");

            GremlinLang.AddStep("length");
            return Wrap<TStart, int?>(this);
        }
        
        /// <summary>
        ///     Adds the length step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd?> Length<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("length", scope);

            GremlinLang.AddStep("length", scope);
            return Wrap<TStart, TNewEnd?>(this);
        }

        /// <summary>
        ///     Adds the limit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Limit<TNewEnd> (Scope scope, long limit)
        {
            Bytecode.AddStep("limit", scope, limit);

            GremlinLang.AddStep("limit", scope, limit);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the limit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Limit<TNewEnd> (long limit)
        {
            Bytecode.AddStep("limit", limit);

            GremlinLang.AddStep("limit", limit);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the local step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Local<TNewEnd> (ITraversal localTraversal)
        {
            Bytecode.AddStep("local", localTraversal);

            GremlinLang.AddStep("local", localTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the loops step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, int> Loops ()
        {
            Bytecode.AddStep("loops");

            GremlinLang.AddStep("loops");
            return Wrap<TStart, int>(this);
        }

        /// <summary>
        ///     Adds the loops step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, int> Loops (string? loopName)
        {
            Bytecode.AddStep("loops", loopName);

            GremlinLang.AddStep("loops", loopName);
            return Wrap<TStart, int>(this);
        }

        /// <summary>
        ///     Adds the lTrim step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string?> LTrim ()
        {
            Bytecode.AddStep("lTrim");

            GremlinLang.AddStep("lTrim");
            return Wrap<TStart, string?>(this);
        }
        
        /// <summary>
        ///     Adds the lTrim step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd?> LTrim<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("lTrim", scope);

            GremlinLang.AddStep("lTrim", scope);
            return Wrap<TStart, TNewEnd?>(this);
        }

        /// <summary>
        ///     Adds the map step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Map<TNewEnd> (IFunction? function)
        {
            Bytecode.AddStep("map", function);

            GremlinLang.AddStep("map", function);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the map step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Map<TNewEnd> (ITraversal mapTraversal)
        {
            Bytecode.AddStep("map", mapTraversal);

            GremlinLang.AddStep("map", mapTraversal);
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

            GremlinLang.AddStep("match", args.ToArray());
            return Wrap<TStart, IDictionary<string, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the math step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, double> Math (string expression)
        {
            Bytecode.AddStep("math", expression);

            GremlinLang.AddStep("math", expression);
            return Wrap<TStart, double>(this);
        }

        /// <summary>
        ///     Adds the max step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Max<TNewEnd> ()
        {
            Bytecode.AddStep("max");

            GremlinLang.AddStep("max");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the max step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Max<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("max", scope);

            GremlinLang.AddStep("max", scope);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the mean step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Mean<TNewEnd> ()
        {
            Bytecode.AddStep("mean");

            GremlinLang.AddStep("mean");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the mean step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Mean<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("mean", scope);

            GremlinLang.AddStep("mean", scope);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the merge step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Merge (object mergeObject)
        {
            Bytecode.AddStep("merge", mergeObject);

            GremlinLang.AddStep("merge", mergeObject);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the mergeE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> MergeE ()
        {
            Bytecode.AddStep("mergeE");

            GremlinLang.AddStep("mergeE");
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the mergeE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> MergeE (IDictionary<object,object?>? m)
        {
            if (m != null && m[Direction.Out] is Vertex outV) {
                m[Direction.Out] = outV.Id;
            }
            if (m != null && m[Direction.In] is Vertex inV) {
                m[Direction.In] = inV.Id;
            }
            Bytecode.AddStep("mergeE", m);

            GremlinLang.AddStep("mergeE", m);
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the mergeE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> MergeE (ITraversal? t)
        {
            Bytecode.AddStep("mergeE", t);

            GremlinLang.AddStep("mergeE", t);
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the mergeV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> MergeV ()
        {
            Bytecode.AddStep("mergeV");

            GremlinLang.AddStep("mergeV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the mergeV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> MergeV (IDictionary<object,object>? m)
        {
            Bytecode.AddStep("mergeV", m);

            GremlinLang.AddStep("mergeV", m);
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the mergeV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> MergeV (ITraversal? t)
        {
            Bytecode.AddStep("mergeV", t);

            GremlinLang.AddStep("mergeV", t);
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the min step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Min<TNewEnd> ()
        {
            Bytecode.AddStep("min");

            GremlinLang.AddStep("min");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the min step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Min<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("min", scope);

            GremlinLang.AddStep("min", scope);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the none step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> None (P? predicate)
        {
            Bytecode.AddStep("none", predicate);

            GremlinLang.AddStep("none", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the not step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Not (ITraversal notTraversal)
        {
            Bytecode.AddStep("not", notTraversal);

            GremlinLang.AddStep("not", notTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the option step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Option (object pickToken, ITraversal? traversalOption)
        {
            Bytecode.AddStep("option", pickToken, traversalOption);

            GremlinLang.AddStep("option", pickToken, traversalOption);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the option step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Option (object pickToken, IDictionary<object,object>? traversalOption)
        {
            Bytecode.AddStep("option", pickToken, traversalOption);

            GremlinLang.AddStep("option", pickToken, traversalOption);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the option step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Option (object pickToken, IDictionary<object,object> traversalOption, Cardinality cardinality)
        {
            Bytecode.AddStep("option", pickToken, traversalOption, cardinality);

            GremlinLang.AddStep("option", pickToken, traversalOption, cardinality);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the option step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Option (ITraversal? traversalOption)
        {
            Bytecode.AddStep("option", traversalOption);

            GremlinLang.AddStep("option", traversalOption);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the optional step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Optional<TNewEnd> (ITraversal optionalTraversal)
        {
            Bytecode.AddStep("optional", optionalTraversal);

            GremlinLang.AddStep("optional", optionalTraversal);
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

            GremlinLang.AddStep("or", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the order step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Order ()
        {
            Bytecode.AddStep("order");

            GremlinLang.AddStep("order");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the order step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Order (Scope scope)
        {
            Bytecode.AddStep("order", scope);

            GremlinLang.AddStep("order", scope);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the otherV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> OtherV ()
        {
            Bytecode.AddStep("otherV");

            GremlinLang.AddStep("otherV");
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

            GremlinLang.AddStep("out", args.ToArray());
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

            GremlinLang.AddStep("outE", args.ToArray());
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the outV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> OutV ()
        {
            Bytecode.AddStep("outV");

            GremlinLang.AddStep("outV");
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the pageRank step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> PageRank ()
        {
            Bytecode.AddStep("pageRank");

            GremlinLang.AddStep("pageRank");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the pageRank step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> PageRank (double alpha)
        {
            Bytecode.AddStep("pageRank", alpha);

            GremlinLang.AddStep("pageRank", alpha);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the path step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Path> Path ()
        {
            Bytecode.AddStep("path");

            GremlinLang.AddStep("path");
            return Wrap<TStart, Path>(this);
        }

        /// <summary>
        ///     Adds the peerPressure step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> PeerPressure ()
        {
            Bytecode.AddStep("peerPressure");

            GremlinLang.AddStep("peerPressure");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the product step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Product (object productObject)
        {
            Bytecode.AddStep("product", productObject);

            GremlinLang.AddStep("product", productObject);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the profile step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Profile<TNewEnd> ()
        {
            Bytecode.AddStep("profile");

            GremlinLang.AddStep("profile");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the profile step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Profile (string sideEffectKey)
        {
            Bytecode.AddStep("profile", sideEffectKey);

            GremlinLang.AddStep("profile", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the program step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Program (object vertexProgram)
        {
            Bytecode.AddStep("program", vertexProgram);

            GremlinLang.AddStep("program", vertexProgram);
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

            GremlinLang.AddStep("project", args.ToArray());
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

            GremlinLang.AddStep("properties", args.ToArray());
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

            GremlinLang.AddStep("property", args.ToArray());
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

            GremlinLang.AddStep("property", args.ToArray());
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the property step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Property (IDictionary<object, object> map)
        {
            Bytecode.AddStep("property", map);

            GremlinLang.AddStep("property", map);
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

            GremlinLang.AddStep("propertyMap", args.ToArray());
            return Wrap<TStart, IDictionary<string, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the range step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Range<TNewEnd> (Scope scope, long low, long high)
        {
            Bytecode.AddStep("range", scope, low, high);

            GremlinLang.AddStep("range", scope, low, high);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the range step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Range<TNewEnd> (long low, long high)
        {
            Bytecode.AddStep("range", low, high);

            GremlinLang.AddStep("range", low, high);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the read step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Read ()
        {
            Bytecode.AddStep("read");

            GremlinLang.AddStep("read");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the repeat step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Repeat (string loopName, ITraversal repeatTraversal)
        {
            Bytecode.AddStep("repeat", loopName, repeatTraversal);

            GremlinLang.AddStep("repeat", loopName, repeatTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the repeat step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Repeat (ITraversal repeatTraversal)
        {
            Bytecode.AddStep("repeat", repeatTraversal);

            GremlinLang.AddStep("repeat", repeatTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the replace step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string?> Replace (string? oldChar, string? newChar)
        {
            Bytecode.AddStep("replace", oldChar, newChar);

            GremlinLang.AddStep("replace", oldChar, newChar);
            return Wrap<TStart, string?>(this);
        }
        
        /// <summary>
        ///     Adds the replace step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd?> Replace<TNewEnd> (Scope scope, string? oldChar, string? newChar)
        {
            Bytecode.AddStep("replace", scope, oldChar, newChar);

            GremlinLang.AddStep("replace", scope, oldChar, newChar);
            return Wrap<TStart, TNewEnd?>(this);
        }

        /// <summary>
        ///     Adds the reverse step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Reverse ()
        {
            Bytecode.AddStep("reverse");

            GremlinLang.AddStep("reverse");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the rTrim step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string?> RTrim ()
        {
            Bytecode.AddStep("rTrim");

            GremlinLang.AddStep("rTrim");
            return Wrap<TStart, string?>(this);
        }
        
        /// <summary>
        ///     Adds the rTrim step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd?> RTrim<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("rTrim", scope);

            GremlinLang.AddStep("rTrim", scope);
            return Wrap<TStart, TNewEnd?>(this);
        }

        /// <summary>
        ///     Adds the sack step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Sack<TNewEnd> ()
        {
            Bytecode.AddStep("sack");

            GremlinLang.AddStep("sack");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the sack step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Sack (IBiFunction sackOperator)
        {
            Bytecode.AddStep("sack", sackOperator);

            GremlinLang.AddStep("sack", sackOperator);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the sample step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Sample (Scope scope, int amountToSample)
        {
            Bytecode.AddStep("sample", scope, amountToSample);

            GremlinLang.AddStep("sample", scope, amountToSample);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the sample step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Sample (int amountToSample)
        {
            Bytecode.AddStep("sample", amountToSample);

            GremlinLang.AddStep("sample", amountToSample);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, ICollection<TNewEnd>> Select<TNewEnd> (Column column)
        {
            Bytecode.AddStep("select", column);

            GremlinLang.AddStep("select", column);
            return Wrap<TStart, ICollection<TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Select<TNewEnd> (Pop pop, string? selectKey)
        {
            Bytecode.AddStep("select", pop, selectKey);

            GremlinLang.AddStep("select", pop, selectKey);
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

            GremlinLang.AddStep("select", args.ToArray());
            return Wrap<TStart, IDictionary<string, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Select<TNewEnd> (Pop pop, ITraversal keyTraversal)
        {
            Bytecode.AddStep("select", pop, keyTraversal);

            GremlinLang.AddStep("select", pop, keyTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Select<TNewEnd> (string? selectKey)
        {
            Bytecode.AddStep("select", selectKey);

            GremlinLang.AddStep("select", selectKey);
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

            GremlinLang.AddStep("select", args.ToArray());
            return Wrap<TStart, IDictionary<string, TNewEnd>>(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Select<TNewEnd> (ITraversal keyTraversal)
        {
            Bytecode.AddStep("select", keyTraversal);

            GremlinLang.AddStep("select", keyTraversal);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the shortestPath step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Path> ShortestPath ()
        {
            Bytecode.AddStep("shortestPath");

            GremlinLang.AddStep("shortestPath");
            return Wrap<TStart, Path>(this);
        }

        /// <summary>
        ///     Adds the sideEffect step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> SideEffect (IConsumer? consumer)
        {
            Bytecode.AddStep("sideEffect", consumer);

            GremlinLang.AddStep("sideEffect", consumer);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the sideEffect step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> SideEffect (ITraversal sideEffectTraversal)
        {
            Bytecode.AddStep("sideEffect", sideEffectTraversal);

            GremlinLang.AddStep("sideEffect", sideEffectTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the simplePath step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> SimplePath ()
        {
            Bytecode.AddStep("simplePath");

            GremlinLang.AddStep("simplePath");
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the skip step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Skip<TNewEnd> (Scope scope, long skip)
        {
            Bytecode.AddStep("skip", scope, skip);

            GremlinLang.AddStep("skip", scope, skip);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the skip step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Skip<TNewEnd> (long skip)
        {
            Bytecode.AddStep("skip", skip);

            GremlinLang.AddStep("skip", skip);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the split step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, List<string>?> Split (string? splitChar)
        {
            Bytecode.AddStep("split", splitChar);

            GremlinLang.AddStep("split", splitChar);
            return Wrap<TStart, List<string>?>(this);
        }
        
        /// <summary>
        ///     Adds the split step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, List<TNewEnd>?> Split<TNewEnd> (Scope scope, string? splitChar)
        {
            Bytecode.AddStep("split", scope, splitChar);

            GremlinLang.AddStep("split", scope, splitChar);
            return Wrap<TStart, List<TNewEnd>?>(this);
        }

        /// <summary>
        ///     Adds the subgraph step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Edge> Subgraph (string sideEffectKey)
        {
            Bytecode.AddStep("subgraph", sideEffectKey);

            GremlinLang.AddStep("subgraph", sideEffectKey);
            return Wrap<TStart, Edge>(this);
        }

        /// <summary>
        ///     Adds the subgraph step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string?> Substring (int startIndex)
        {
            Bytecode.AddStep("substring", startIndex);

            GremlinLang.AddStep("substring", startIndex);
            return Wrap<TStart, string?>(this);
        }
        
        /// <summary>
        ///     Adds the subgraph step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd?> Substring<TNewEnd> (Scope scope, int startIndex)
        {
            Bytecode.AddStep("substring", scope,  startIndex);

            GremlinLang.AddStep("substring", scope,  startIndex);
            return Wrap<TStart, TNewEnd?>(this);
        }

        /// <summary>
        ///     Adds the subgraph step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string?> Substring (int startIndex, int endIndex)
        {
            Bytecode.AddStep("substring", startIndex, endIndex);

            GremlinLang.AddStep("substring", startIndex, endIndex);
            return Wrap<TStart, string?>(this);
        }
        
        /// <summary>
        ///     Adds the subgraph step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd?> Substring<TNewEnd> (Scope scope, int startIndex, int endIndex)
        {
            Bytecode.AddStep("substring", scope, startIndex, endIndex);

            GremlinLang.AddStep("substring", scope, startIndex, endIndex);
            return Wrap<TStart, TNewEnd?>(this);
        }

        /// <summary>
        ///     Adds the sum step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Sum<TNewEnd> ()
        {
            Bytecode.AddStep("sum");

            GremlinLang.AddStep("sum");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the sum step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Sum<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("sum", scope);

            GremlinLang.AddStep("sum", scope);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the tail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Tail<TNewEnd> ()
        {
            Bytecode.AddStep("tail");

            GremlinLang.AddStep("tail");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the tail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Tail<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("tail", scope);

            GremlinLang.AddStep("tail", scope);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the tail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Tail<TNewEnd> (Scope scope, long limit)
        {
            Bytecode.AddStep("tail", scope, limit);

            GremlinLang.AddStep("tail", scope, limit);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the tail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Tail<TNewEnd> (long limit)
        {
            Bytecode.AddStep("tail", limit);

            GremlinLang.AddStep("tail", limit);
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the timeLimit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> TimeLimit (long timeLimit)
        {
            Bytecode.AddStep("timeLimit", timeLimit);

            GremlinLang.AddStep("timeLimit", timeLimit);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the times step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Times (int maxLoops)
        {
            Bytecode.AddStep("times", maxLoops);

            GremlinLang.AddStep("times", maxLoops);
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

            GremlinLang.AddStep("to", args.ToArray());
            return Wrap<TStart, Vertex>(this);
        }
        
        /// <summary>
        ///     Adds the to step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> To (object? toStepVertexIdOrLabel)
        {
            var toId = toStepVertexIdOrLabel is Vertex toVertex ? toVertex.Id : toStepVertexIdOrLabel;
            Bytecode.AddStep("to", toId);
            GremlinLang.AddStep("to", toId);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the to step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> To (string? toStepLabel)
        {
            Bytecode.AddStep("to", toStepLabel);

            GremlinLang.AddStep("to", toStepLabel);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the to step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> To (ITraversal toVertex)
        {
            Bytecode.AddStep("to", toVertex);

            GremlinLang.AddStep("to", toVertex);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the to step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> To (Vertex? toVertex)
        {
            Bytecode.AddStep("to", toVertex?.Id);

            GremlinLang.AddStep("to", toVertex?.Id);
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

            GremlinLang.AddStep("toE", args.ToArray());
            return Wrap<TStart, Edge>(this);
        }
        
        /// <summary>
        ///     Adds the toLower step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string?> ToLower ()
        {
            Bytecode.AddStep("toLower");

            GremlinLang.AddStep("toLower");
            return Wrap<TStart, string?>(this);
        }
        
        /// <summary>
        ///     Adds the toLower step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd?> ToLower<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("toLower", scope);

            GremlinLang.AddStep("toLower", scope);
            return Wrap<TStart, TNewEnd?>(this);
        }
        
        /// <summary>
        ///     Adds the toUpper step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string?> ToUpper ()
        {
            Bytecode.AddStep("toUpper");

            GremlinLang.AddStep("toUpper");
            return Wrap<TStart, string?>(this);
        }
        
        /// <summary>
        ///     Adds the toUpper step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd?> ToUpper<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("toUpper", scope);

            GremlinLang.AddStep("toUpper", scope);
            return Wrap<TStart, TNewEnd?>(this);
        }

        /// <summary>
        ///     Adds the toV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, Vertex> ToV (Direction? direction)
        {
            Bytecode.AddStep("toV", direction);

            GremlinLang.AddStep("toV", direction);
            return Wrap<TStart, Vertex>(this);
        }

        /// <summary>
        ///     Adds the tree step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Tree<TNewEnd> ()
        {
            Bytecode.AddStep("tree");

            GremlinLang.AddStep("tree");
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the tree step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Tree (string sideEffectKey)
        {
            Bytecode.AddStep("tree", sideEffectKey);

            GremlinLang.AddStep("tree", sideEffectKey);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the trim step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, string?> Trim ()
        {
            Bytecode.AddStep("trim");

            GremlinLang.AddStep("trim");
            return Wrap<TStart, string?>(this);
        }
        
        /// <summary>
        ///     Adds the trim step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd?> Trim<TNewEnd> (Scope scope)
        {
            Bytecode.AddStep("trim", scope);

            GremlinLang.AddStep("trim", scope);
            return Wrap<TStart, TNewEnd?>(this);
        }

        /// <summary>
        ///     Adds the unfold step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Unfold<TNewEnd> ()
        {
            Bytecode.AddStep("unfold");

            GremlinLang.AddStep("unfold");
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

            GremlinLang.AddStep("union", args.ToArray());
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the until step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Until (IPredicate untilPredicate)
        {
            Bytecode.AddStep("until", untilPredicate);

            GremlinLang.AddStep("until", untilPredicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the until step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Until (ITraversal untilTraversal)
        {
            Bytecode.AddStep("until", untilTraversal);

            GremlinLang.AddStep("until", untilTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the value step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TNewEnd> Value<TNewEnd> ()
        {
            Bytecode.AddStep("value");

            GremlinLang.AddStep("value");
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

            GremlinLang.AddStep("valueMap", args.ToArray());
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

            GremlinLang.AddStep("valueMap", args.ToArray());
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

            GremlinLang.AddStep("values", args.ToArray());
            return Wrap<TStart, TNewEnd>(this);
        }

        /// <summary>
        ///     Adds the where step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Where (P predicate)
        {
            Bytecode.AddStep("where", predicate);

            GremlinLang.AddStep("where", predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the where step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Where (string startKey, P predicate)
        {
            Bytecode.AddStep("where", startKey, predicate);

            GremlinLang.AddStep("where", startKey, predicate);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the where step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Where (ITraversal whereTraversal)
        {
            Bytecode.AddStep("where", whereTraversal);

            GremlinLang.AddStep("where", whereTraversal);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the with step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> With (string key)
        {
            Bytecode.AddStep("with", key);

            GremlinLang.AddStep("with", key);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the with step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> With (string key, object? value)
        {
            Bytecode.AddStep("with", key, value);

            GremlinLang.AddStep("with", key, value);
            return Wrap<TStart, TEnd>(this);
        }

        /// <summary>
        ///     Adds the write step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Write ()
        {
            Bytecode.AddStep("write");

            GremlinLang.AddStep("write");
            return Wrap<TStart, TEnd>(this);
        }


        /// <summary>
        /// Make a copy of a traversal that is reset for iteration.
        /// </summary>
        public GraphTraversal<TStart, TEnd> Clone()
        {
            return new GraphTraversal<TStart, TEnd>(TraversalStrategies, new Bytecode(Bytecode), GremlinLang.Clone());
        }
    }
}