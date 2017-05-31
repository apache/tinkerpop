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

namespace Gremlin.Net.Process.Traversal
{
    public static class __
    {
        public static GraphTraversal<object, object> Start()
        {
            return new GraphTraversal<object, object>();
        }

        public static GraphTraversal<object, Vertex> V(params object[] args)
        {
            return new GraphTraversal<object, object>().V(args);
        }

        public static GraphTraversal<object, Edge> AddE(params object[] args)
        {
            return new GraphTraversal<object, object>().AddE(args);
        }

        public static GraphTraversal<object, Edge> AddInE(params object[] args)
        {
            return new GraphTraversal<object, object>().AddInE(args);
        }

        public static GraphTraversal<object, Edge> AddOutE(params object[] args)
        {
            return new GraphTraversal<object, object>().AddOutE(args);
        }

        public static GraphTraversal<object, Vertex> AddV(params object[] args)
        {
            return new GraphTraversal<object, object>().AddV(args);
        }

        public static GraphTraversal<object, object> Aggregate(params object[] args)
        {
            return new GraphTraversal<object, object>().Aggregate(args);
        }

        public static GraphTraversal<object, object> And(params object[] args)
        {
            return new GraphTraversal<object, object>().And(args);
        }

        public static GraphTraversal<object, object> As(params object[] args)
        {
            return new GraphTraversal<object, object>().As(args);
        }

        public static GraphTraversal<object, object> Barrier(params object[] args)
        {
            return new GraphTraversal<object, object>().Barrier(args);
        }

        public static GraphTraversal<object, Vertex> Both(params object[] args)
        {
            return new GraphTraversal<object, object>().Both(args);
        }

        public static GraphTraversal<object, Edge> BothE(params object[] args)
        {
            return new GraphTraversal<object, object>().BothE(args);
        }

        public static GraphTraversal<object, Vertex> BothV(params object[] args)
        {
            return new GraphTraversal<object, object>().BothV(args);
        }

        public static GraphTraversal<object, E2> Branch<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Branch<E2>(args);
        }

        public static GraphTraversal<object, E2> Cap<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Cap<E2>(args);
        }

        public static GraphTraversal<object, E2> Choose<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Choose<E2>(args);
        }

        public static GraphTraversal<object, E2> Coalesce<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Coalesce<E2>(args);
        }

        public static GraphTraversal<object, object> Coin(params object[] args)
        {
            return new GraphTraversal<object, object>().Coin(args);
        }

        public static GraphTraversal<object, E2> Constant<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Constant<E2>(args);
        }

        public static GraphTraversal<object, long> Count(params object[] args)
        {
            return new GraphTraversal<object, object>().Count(args);
        }

        public static GraphTraversal<object, object> CyclicPath(params object[] args)
        {
            return new GraphTraversal<object, object>().CyclicPath(args);
        }

        public static GraphTraversal<object, object> Dedup(params object[] args)
        {
            return new GraphTraversal<object, object>().Dedup(args);
        }

        public static GraphTraversal<object, object> Drop(params object[] args)
        {
            return new GraphTraversal<object, object>().Drop(args);
        }

        public static GraphTraversal<object, object> Emit(params object[] args)
        {
            return new GraphTraversal<object, object>().Emit(args);
        }

        public static GraphTraversal<object, object> Filter(params object[] args)
        {
            return new GraphTraversal<object, object>().Filter(args);
        }

        public static GraphTraversal<object, E2> FlatMap<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().FlatMap<E2>(args);
        }

        public static GraphTraversal<object, E2> Fold<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Fold<E2>(args);
        }

        public static GraphTraversal<object, object> Group(params object[] args)
        {
            return new GraphTraversal<object, object>().Group(args);
        }

        public static GraphTraversal<object, object> GroupCount(params object[] args)
        {
            return new GraphTraversal<object, object>().GroupCount(args);
        }

        public static GraphTraversal<object, object> GroupV3d0(params object[] args)
        {
            return new GraphTraversal<object, object>().GroupV3d0(args);
        }

        public static GraphTraversal<object, object> Has(params object[] args)
        {
            return new GraphTraversal<object, object>().Has(args);
        }

        public static GraphTraversal<object, object> HasId(params object[] args)
        {
            return new GraphTraversal<object, object>().HasId(args);
        }

        public static GraphTraversal<object, object> HasKey(params object[] args)
        {
            return new GraphTraversal<object, object>().HasKey(args);
        }

        public static GraphTraversal<object, object> HasLabel(params object[] args)
        {
            return new GraphTraversal<object, object>().HasLabel(args);
        }

        public static GraphTraversal<object, object> HasNot(params object[] args)
        {
            return new GraphTraversal<object, object>().HasNot(args);
        }

        public static GraphTraversal<object, object> HasValue(params object[] args)
        {
            return new GraphTraversal<object, object>().HasValue(args);
        }

        public static GraphTraversal<object, object> Id(params object[] args)
        {
            return new GraphTraversal<object, object>().Id(args);
        }

        public static GraphTraversal<object, object> Identity(params object[] args)
        {
            return new GraphTraversal<object, object>().Identity(args);
        }

        public static GraphTraversal<object, Vertex> In(params object[] args)
        {
            return new GraphTraversal<object, object>().In(args);
        }

        public static GraphTraversal<object, Edge> InE(params object[] args)
        {
            return new GraphTraversal<object, object>().InE(args);
        }

        public static GraphTraversal<object, Vertex> InV(params object[] args)
        {
            return new GraphTraversal<object, object>().InV(args);
        }

        public static GraphTraversal<object, object> Inject(params object[] args)
        {
            return new GraphTraversal<object, object>().Inject(args);
        }

        public static GraphTraversal<object, object> Is(params object[] args)
        {
            return new GraphTraversal<object, object>().Is(args);
        }

        public static GraphTraversal<object, string> Key(params object[] args)
        {
            return new GraphTraversal<object, object>().Key(args);
        }

        public static GraphTraversal<object, string> Label(params object[] args)
        {
            return new GraphTraversal<object, object>().Label(args);
        }

        public static GraphTraversal<object, E2> Limit<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Limit<E2>(args);
        }

        public static GraphTraversal<object, E2> Local<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Local<E2>(args);
        }

        public static GraphTraversal<object, int> Loops(params object[] args)
        {
            return new GraphTraversal<object, object>().Loops(args);
        }

        public static GraphTraversal<object, E2> Map<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Map<E2>(args);
        }

        public static GraphTraversal<object, E2> MapKeys<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().MapKeys<E2>(args);
        }

        public static GraphTraversal<object, E2> MapValues<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().MapValues<E2>(args);
        }

        public static GraphTraversal<object, IDictionary<string, E2>> Match<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Match<E2>(args);
        }

        public static GraphTraversal<object, E2> Max<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Max<E2>(args);
        }

        public static GraphTraversal<object, E2> Mean<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Mean<E2>(args);
        }

        public static GraphTraversal<object, E2> Min<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Min<E2>(args);
        }

        public static GraphTraversal<object, object> Not(params object[] args)
        {
            return new GraphTraversal<object, object>().Not(args);
        }

        public static GraphTraversal<object, E2> Optional<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Optional<E2>(args);
        }

        public static GraphTraversal<object, object> Or(params object[] args)
        {
            return new GraphTraversal<object, object>().Or(args);
        }

        public static GraphTraversal<object, object> Order(params object[] args)
        {
            return new GraphTraversal<object, object>().Order(args);
        }

        public static GraphTraversal<object, Vertex> OtherV(params object[] args)
        {
            return new GraphTraversal<object, object>().OtherV(args);
        }

        public static GraphTraversal<object, Vertex> Out(params object[] args)
        {
            return new GraphTraversal<object, object>().Out(args);
        }

        public static GraphTraversal<object, Edge> OutE(params object[] args)
        {
            return new GraphTraversal<object, object>().OutE(args);
        }

        public static GraphTraversal<object, Vertex> OutV(params object[] args)
        {
            return new GraphTraversal<object, object>().OutV(args);
        }

        public static GraphTraversal<object, Path> Path(params object[] args)
        {
            return new GraphTraversal<object, object>().Path(args);
        }

        public static GraphTraversal<object, IDictionary<string, E2>> Project<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Project<E2>(args);
        }

        public static GraphTraversal<object, E2> Properties<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Properties<E2>(args);
        }

        public static GraphTraversal<object, object> Property(params object[] args)
        {
            return new GraphTraversal<object, object>().Property(args);
        }

        public static GraphTraversal<object, IDictionary<string, E2>> PropertyMap<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().PropertyMap<E2>(args);
        }

        public static GraphTraversal<object, E2> Range<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Range<E2>(args);
        }

        public static GraphTraversal<object, object> Repeat(params object[] args)
        {
            return new GraphTraversal<object, object>().Repeat(args);
        }

        public static GraphTraversal<object, object> Sack(params object[] args)
        {
            return new GraphTraversal<object, object>().Sack(args);
        }

        public static GraphTraversal<object, object> Sample(params object[] args)
        {
            return new GraphTraversal<object, object>().Sample(args);
        }

        public static GraphTraversal<object, IDictionary<string, E2>> Select<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Select<E2>(args);
        }

        public static GraphTraversal<object, object> SideEffect(params object[] args)
        {
            return new GraphTraversal<object, object>().SideEffect(args);
        }

        public static GraphTraversal<object, object> SimplePath(params object[] args)
        {
            return new GraphTraversal<object, object>().SimplePath(args);
        }

        public static GraphTraversal<object, object> Store(params object[] args)
        {
            return new GraphTraversal<object, object>().Store(args);
        }

        public static GraphTraversal<object, Edge> Subgraph(params object[] args)
        {
            return new GraphTraversal<object, object>().Subgraph(args);
        }

        public static GraphTraversal<object, E2> Sum<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Sum<E2>(args);
        }

        public static GraphTraversal<object, E2> Tail<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Tail<E2>(args);
        }

        public static GraphTraversal<object, object> TimeLimit(params object[] args)
        {
            return new GraphTraversal<object, object>().TimeLimit(args);
        }

        public static GraphTraversal<object, object> Times(params object[] args)
        {
            return new GraphTraversal<object, object>().Times(args);
        }

        public static GraphTraversal<object, Vertex> To(params object[] args)
        {
            return new GraphTraversal<object, object>().To(args);
        }

        public static GraphTraversal<object, Edge> ToE(params object[] args)
        {
            return new GraphTraversal<object, object>().ToE(args);
        }

        public static GraphTraversal<object, Vertex> ToV(params object[] args)
        {
            return new GraphTraversal<object, object>().ToV(args);
        }

        public static GraphTraversal<object, object> Tree(params object[] args)
        {
            return new GraphTraversal<object, object>().Tree(args);
        }

        public static GraphTraversal<object, E2> Unfold<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Unfold<E2>(args);
        }

        public static GraphTraversal<object, E2> Union<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Union<E2>(args);
        }

        public static GraphTraversal<object, object> Until(params object[] args)
        {
            return new GraphTraversal<object, object>().Until(args);
        }

        public static GraphTraversal<object, E2> Value<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Value<E2>(args);
        }

        public static GraphTraversal<object, IDictionary<string, E2>> ValueMap<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().ValueMap<E2>(args);
        }

        public static GraphTraversal<object, E2> Values<E2>(params object[] args)
        {
            return new GraphTraversal<object, object>().Values<E2>(args);
        }

        public static GraphTraversal<object, object> Where(params object[] args)
        {
            return new GraphTraversal<object, object>().Where(args);
        }
    }
}
