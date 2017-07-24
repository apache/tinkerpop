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

using System.Collections.Generic;
using Gremlin.Net.Structure;

// THIS IS A GENERATED FILE - DO NOT MODIFY THIS FILE DIRECTLY - see pom.xml
namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     An anonymous <see cref="GraphTraversal{SType, EType}" />.
    /// </summary>
    public static class __
    {
        /// <summary>
        ///     Starts an empty <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public static GraphTraversal<object, object> Start()
        {
            return new GraphTraversal<object, object>();
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the V step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> V(params object[] args)
        {
            return new GraphTraversal<object, object>().V(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the addE step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Edge> AddE(params object[] args)
        {
            return new GraphTraversal<object, object>().AddE(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the addV step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> AddV(params object[] args)
        {
            return new GraphTraversal<object, object>().AddV(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the aggregate step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Aggregate(params object[] args)
        {
            return new GraphTraversal<object, object>().Aggregate(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the and step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> And(params object[] args)
        {
            return new GraphTraversal<object, object>().And(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the as step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> As(params object[] args)
        {
            return new GraphTraversal<object, object>().As(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the barrier step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Barrier(params object[] args)
        {
            return new GraphTraversal<object, object>().Barrier(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the both step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> Both(params object[] args)
        {
            return new GraphTraversal<object, object>().Both(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the bothE step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Edge> BothE(params object[] args)
        {
            return new GraphTraversal<object, object>().BothE(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the bothV step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> BothV(params object[] args)
        {
            return new GraphTraversal<object, object>().BothV(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the branch step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Branch<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Branch<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the cap step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Cap<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Cap<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the choose step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Choose<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Choose<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the coalesce step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Coalesce<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Coalesce<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the coin step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Coin(params object[] args)
        {
            return new GraphTraversal<object, object>().Coin(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the constant step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Constant<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Constant<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the count step to that traversal.
        /// </summary>
        public static GraphTraversal<object, long> Count(params object[] args)
        {
            return new GraphTraversal<object, object>().Count(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the cyclicPath step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> CyclicPath(params object[] args)
        {
            return new GraphTraversal<object, object>().CyclicPath(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the dedup step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Dedup(params object[] args)
        {
            return new GraphTraversal<object, object>().Dedup(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the drop step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Drop(params object[] args)
        {
            return new GraphTraversal<object, object>().Drop(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the emit step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Emit(params object[] args)
        {
            return new GraphTraversal<object, object>().Emit(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the filter step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Filter(params object[] args)
        {
            return new GraphTraversal<object, object>().Filter(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the flatMap step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> FlatMap<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().FlatMap<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the fold step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Fold<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Fold<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the group step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Group(params object[] args)
        {
            return new GraphTraversal<object, object>().Group(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the groupCount step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> GroupCount(params object[] args)
        {
            return new GraphTraversal<object, object>().GroupCount(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the has step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Has(params object[] args)
        {
            return new GraphTraversal<object, object>().Has(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the hasId step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> HasId(params object[] args)
        {
            return new GraphTraversal<object, object>().HasId(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the hasKey step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> HasKey(params object[] args)
        {
            return new GraphTraversal<object, object>().HasKey(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the hasLabel step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> HasLabel(params object[] args)
        {
            return new GraphTraversal<object, object>().HasLabel(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the hasNot step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> HasNot(params object[] args)
        {
            return new GraphTraversal<object, object>().HasNot(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the hasValue step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> HasValue(params object[] args)
        {
            return new GraphTraversal<object, object>().HasValue(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the id step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Id(params object[] args)
        {
            return new GraphTraversal<object, object>().Id(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the identity step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Identity(params object[] args)
        {
            return new GraphTraversal<object, object>().Identity(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the in step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> In(params object[] args)
        {
            return new GraphTraversal<object, object>().In(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the inE step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Edge> InE(params object[] args)
        {
            return new GraphTraversal<object, object>().InE(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the inV step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> InV(params object[] args)
        {
            return new GraphTraversal<object, object>().InV(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the inject step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Inject(params object[] args)
        {
            return new GraphTraversal<object, object>().Inject(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the is step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Is(params object[] args)
        {
            return new GraphTraversal<object, object>().Is(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the key step to that traversal.
        /// </summary>
        public static GraphTraversal<object, string> Key(params object[] args)
        {
            return new GraphTraversal<object, object>().Key(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the label step to that traversal.
        /// </summary>
        public static GraphTraversal<object, string> Label(params object[] args)
        {
            return new GraphTraversal<object, object>().Label(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the limit step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Limit<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Limit<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the local step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Local<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Local<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the loops step to that traversal.
        /// </summary>
        public static GraphTraversal<object, int> Loops(params object[] args)
        {
            return new GraphTraversal<object, object>().Loops(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the map step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Map<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Map<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the match step to that traversal.
        /// </summary>
        public static GraphTraversal<object, IDictionary<string, E2>> Match<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Match<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the max step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Max<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Max<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the mean step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Mean<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Mean<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the min step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Min<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Min<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the not step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Not(params object[] args)
        {
            return new GraphTraversal<object, object>().Not(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the optional step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Optional<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Optional<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the or step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Or(params object[] args)
        {
            return new GraphTraversal<object, object>().Or(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the order step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Order(params object[] args)
        {
            return new GraphTraversal<object, object>().Order(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the otherV step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> OtherV(params object[] args)
        {
            return new GraphTraversal<object, object>().OtherV(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the out step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> Out(params object[] args)
        {
            return new GraphTraversal<object, object>().Out(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the outE step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Edge> OutE(params object[] args)
        {
            return new GraphTraversal<object, object>().OutE(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the outV step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> OutV(params object[] args)
        {
            return new GraphTraversal<object, object>().OutV(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the path step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Path> Path(params object[] args)
        {
            return new GraphTraversal<object, object>().Path(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the project step to that traversal.
        /// </summary>
        public static GraphTraversal<object, IDictionary<string, E2>> Project<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Project<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the properties step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Properties<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Properties<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the property step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Property(params object[] args)
        {
            return new GraphTraversal<object, object>().Property(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the propertyMap step to that traversal.
        /// </summary>
        public static GraphTraversal<object, IDictionary<string, E2>> PropertyMap<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().PropertyMap<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the range step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Range<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Range<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the repeat step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Repeat(params object[] args)
        {
            return new GraphTraversal<object, object>().Repeat(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the sack step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Sack(params object[] args)
        {
            return new GraphTraversal<object, object>().Sack(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the sample step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Sample(params object[] args)
        {
            return new GraphTraversal<object, object>().Sample(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the select step to that traversal.
        /// </summary>
        public static GraphTraversal<object, IDictionary<string, E2>> Select<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Select<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the sideEffect step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> SideEffect(params object[] args)
        {
            return new GraphTraversal<object, object>().SideEffect(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the simplePath step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> SimplePath(params object[] args)
        {
            return new GraphTraversal<object, object>().SimplePath(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the skip step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Skip<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Skip<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the store step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Store(params object[] args)
        {
            return new GraphTraversal<object, object>().Store(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the subgraph step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Edge> Subgraph(params object[] args)
        {
            return new GraphTraversal<object, object>().Subgraph(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the sum step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Sum<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Sum<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the tail step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Tail<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Tail<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the timeLimit step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> TimeLimit(params object[] args)
        {
            return new GraphTraversal<object, object>().TimeLimit(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the times step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Times(params object[] args)
        {
            return new GraphTraversal<object, object>().Times(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the to step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> To(params object[] args)
        {
            return new GraphTraversal<object, object>().To(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the toE step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Edge> ToE(params object[] args)
        {
            return new GraphTraversal<object, object>().ToE(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the toV step to that traversal.
        /// </summary>
        public static GraphTraversal<object, Vertex> ToV(params object[] args)
        {
            return new GraphTraversal<object, object>().ToV(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the tree step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Tree(params object[] args)
        {
            return new GraphTraversal<object, object>().Tree(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the unfold step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Unfold<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Unfold<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the union step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Union<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Union<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the until step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Until(params object[] args)
        {
            return new GraphTraversal<object, object>().Until(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the value step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Value<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Value<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the valueMap step to that traversal.
        /// </summary>
        public static GraphTraversal<object, IDictionary<TKey, TValue>> ValueMap<TKey, TValue>(params object[] args)
        {
            return new GraphTraversal<object, object>().ValueMap<TKey, TValue>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the values step to that traversal.
        /// </summary>
        public static GraphTraversal<object, E2> Values<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Values<E2>(args);
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> and adds the where step to that traversal.
        /// </summary>
        public static GraphTraversal<object, object> Where(params object[] args)
        {
            return new GraphTraversal<object, object>().Where(args);
        }

    }
}