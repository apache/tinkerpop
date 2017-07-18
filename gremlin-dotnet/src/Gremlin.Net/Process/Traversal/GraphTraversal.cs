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
    ///     Graph traversals are the primary way in which graphs are processed.
    /// </summary>
    public class GraphTraversal<S, E> : DefaultTraversal<S, E>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversal{SType, EType}" /> class.
        /// </summary>
        public GraphTraversal()
            : this(new List<ITraversalStrategy>(), new Bytecode())
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="GraphTraversal{SType, EType}" /> class.
        /// </summary>
        /// <param name="traversalStrategies">The traversal strategies to be used by this graph traversal at evaluation time.</param>
        /// <param name="bytecode">The <see cref="Bytecode" /> associated with the construction of this graph traversal.</param>
        public GraphTraversal(ICollection<ITraversalStrategy> traversalStrategies, Bytecode bytecode)
        {
            TraversalStrategies = traversalStrategies;
            Bytecode = bytecode;
        }

        private static GraphTraversal<S2, E2> Wrap<S2, E2>(GraphTraversal<S, E> traversal)
        {
            if (typeof(S2) == typeof(S) && typeof(E2) == typeof(E))
            {
                return traversal as GraphTraversal<S2, E2>;
            }
            // New wrapper
            return new GraphTraversal<S2, E2>(traversal.TraversalStrategies, traversal.Bytecode);
        }


        /// <summary>
        ///     Adds the V step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > V (params object[] args)
        {
            Bytecode.AddStep("V", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the addE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Edge > AddE (params object[] args)
        {
            Bytecode.AddStep("addE", args);
            return Wrap< S , Edge >(this);
        }

        /// <summary>
        ///     Adds the addInE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Edge > AddInE (params object[] args)
        {
            Bytecode.AddStep("addInE", args);
            return Wrap< S , Edge >(this);
        }

        /// <summary>
        ///     Adds the addOutE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Edge > AddOutE (params object[] args)
        {
            Bytecode.AddStep("addOutE", args);
            return Wrap< S , Edge >(this);
        }

        /// <summary>
        ///     Adds the addV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > AddV (params object[] args)
        {
            Bytecode.AddStep("addV", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the aggregate step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Aggregate (params object[] args)
        {
            Bytecode.AddStep("aggregate", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the and step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > And (params object[] args)
        {
            Bytecode.AddStep("and", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the as step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > As (params object[] args)
        {
            Bytecode.AddStep("as", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the barrier step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Barrier (params object[] args)
        {
            Bytecode.AddStep("barrier", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the both step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > Both (params object[] args)
        {
            Bytecode.AddStep("both", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the bothE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Edge > BothE (params object[] args)
        {
            Bytecode.AddStep("bothE", args);
            return Wrap< S , Edge >(this);
        }

        /// <summary>
        ///     Adds the bothV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > BothV (params object[] args)
        {
            Bytecode.AddStep("bothV", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the branch step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Branch<E2> (params object[] args)
        {
            Bytecode.AddStep("branch", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the by step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > By (params object[] args)
        {
            Bytecode.AddStep("by", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the cap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Cap<E2> (params object[] args)
        {
            Bytecode.AddStep("cap", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the choose step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Choose<E2> (params object[] args)
        {
            Bytecode.AddStep("choose", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the coalesce step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Coalesce<E2> (params object[] args)
        {
            Bytecode.AddStep("coalesce", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the coin step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Coin (params object[] args)
        {
            Bytecode.AddStep("coin", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the constant step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Constant<E2> (params object[] args)
        {
            Bytecode.AddStep("constant", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the count step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , long > Count (params object[] args)
        {
            Bytecode.AddStep("count", args);
            return Wrap< S , long >(this);
        }

        /// <summary>
        ///     Adds the cyclicPath step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > CyclicPath (params object[] args)
        {
            Bytecode.AddStep("cyclicPath", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the dedup step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Dedup (params object[] args)
        {
            Bytecode.AddStep("dedup", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the drop step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Drop (params object[] args)
        {
            Bytecode.AddStep("drop", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the emit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Emit (params object[] args)
        {
            Bytecode.AddStep("emit", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the filter step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Filter (params object[] args)
        {
            Bytecode.AddStep("filter", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the flatMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > FlatMap<E2> (params object[] args)
        {
            Bytecode.AddStep("flatMap", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the fold step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Fold<E2> (params object[] args)
        {
            Bytecode.AddStep("fold", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the from step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > From (params object[] args)
        {
            Bytecode.AddStep("from", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the group step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Group (params object[] args)
        {
            Bytecode.AddStep("group", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the groupCount step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > GroupCount (params object[] args)
        {
            Bytecode.AddStep("groupCount", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the groupV3d0 step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > GroupV3d0 (params object[] args)
        {
            Bytecode.AddStep("groupV3d0", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the has step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Has (params object[] args)
        {
            Bytecode.AddStep("has", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the hasId step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > HasId (params object[] args)
        {
            Bytecode.AddStep("hasId", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the hasKey step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > HasKey (params object[] args)
        {
            Bytecode.AddStep("hasKey", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the hasLabel step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > HasLabel (params object[] args)
        {
            Bytecode.AddStep("hasLabel", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the hasNot step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > HasNot (params object[] args)
        {
            Bytecode.AddStep("hasNot", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the hasValue step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > HasValue (params object[] args)
        {
            Bytecode.AddStep("hasValue", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the id step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , object > Id (params object[] args)
        {
            Bytecode.AddStep("id", args);
            return Wrap< S , object >(this);
        }

        /// <summary>
        ///     Adds the identity step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Identity (params object[] args)
        {
            Bytecode.AddStep("identity", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the in step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > In (params object[] args)
        {
            Bytecode.AddStep("in", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the inE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Edge > InE (params object[] args)
        {
            Bytecode.AddStep("inE", args);
            return Wrap< S , Edge >(this);
        }

        /// <summary>
        ///     Adds the inV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > InV (params object[] args)
        {
            Bytecode.AddStep("inV", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the inject step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Inject (params object[] args)
        {
            Bytecode.AddStep("inject", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the is step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Is (params object[] args)
        {
            Bytecode.AddStep("is", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the key step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , string > Key (params object[] args)
        {
            Bytecode.AddStep("key", args);
            return Wrap< S , string >(this);
        }

        /// <summary>
        ///     Adds the label step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , string > Label (params object[] args)
        {
            Bytecode.AddStep("label", args);
            return Wrap< S , string >(this);
        }

        /// <summary>
        ///     Adds the limit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Limit<E2> (params object[] args)
        {
            Bytecode.AddStep("limit", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the local step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Local<E2> (params object[] args)
        {
            Bytecode.AddStep("local", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the loops step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , int > Loops (params object[] args)
        {
            Bytecode.AddStep("loops", args);
            return Wrap< S , int >(this);
        }

        /// <summary>
        ///     Adds the map step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Map<E2> (params object[] args)
        {
            Bytecode.AddStep("map", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the mapKeys step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > MapKeys<E2> (params object[] args)
        {
            Bytecode.AddStep("mapKeys", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the mapValues step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > MapValues<E2> (params object[] args)
        {
            Bytecode.AddStep("mapValues", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the match step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , IDictionary<string, E2> > Match<E2> (params object[] args)
        {
            Bytecode.AddStep("match", args);
            return Wrap< S , IDictionary<string, E2> >(this);
        }

        /// <summary>
        ///     Adds the max step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Max<E2> (params object[] args)
        {
            Bytecode.AddStep("max", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the mean step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Mean<E2> (params object[] args)
        {
            Bytecode.AddStep("mean", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the min step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Min<E2> (params object[] args)
        {
            Bytecode.AddStep("min", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the not step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Not (params object[] args)
        {
            Bytecode.AddStep("not", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the option step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Option (params object[] args)
        {
            Bytecode.AddStep("option", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the optional step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Optional<E2> (params object[] args)
        {
            Bytecode.AddStep("optional", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the or step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Or (params object[] args)
        {
            Bytecode.AddStep("or", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the order step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Order (params object[] args)
        {
            Bytecode.AddStep("order", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the otherV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > OtherV (params object[] args)
        {
            Bytecode.AddStep("otherV", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the out step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > Out (params object[] args)
        {
            Bytecode.AddStep("out", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the outE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Edge > OutE (params object[] args)
        {
            Bytecode.AddStep("outE", args);
            return Wrap< S , Edge >(this);
        }

        /// <summary>
        ///     Adds the outV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > OutV (params object[] args)
        {
            Bytecode.AddStep("outV", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the pageRank step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > PageRank (params object[] args)
        {
            Bytecode.AddStep("pageRank", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the path step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Path > Path (params object[] args)
        {
            Bytecode.AddStep("path", args);
            return Wrap< S , Path >(this);
        }

        /// <summary>
        ///     Adds the peerPressure step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > PeerPressure (params object[] args)
        {
            Bytecode.AddStep("peerPressure", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the profile step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Profile (params object[] args)
        {
            Bytecode.AddStep("profile", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the program step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Program (params object[] args)
        {
            Bytecode.AddStep("program", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the project step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , IDictionary<string, E2> > Project<E2> (params object[] args)
        {
            Bytecode.AddStep("project", args);
            return Wrap< S , IDictionary<string, E2> >(this);
        }

        /// <summary>
        ///     Adds the properties step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Properties<E2> (params object[] args)
        {
            Bytecode.AddStep("properties", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the property step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Property (params object[] args)
        {
            Bytecode.AddStep("property", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the propertyMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , IDictionary<string, E2> > PropertyMap<E2> (params object[] args)
        {
            Bytecode.AddStep("propertyMap", args);
            return Wrap< S , IDictionary<string, E2> >(this);
        }

        /// <summary>
        ///     Adds the range step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Range<E2> (params object[] args)
        {
            Bytecode.AddStep("range", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the repeat step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Repeat (params object[] args)
        {
            Bytecode.AddStep("repeat", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the sack step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Sack (params object[] args)
        {
            Bytecode.AddStep("sack", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the sample step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Sample (params object[] args)
        {
            Bytecode.AddStep("sample", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the select step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , IDictionary<string, E2> > Select<E2> (params object[] args)
        {
            Bytecode.AddStep("select", args);
            return Wrap< S , IDictionary<string, E2> >(this);
        }

        /// <summary>
        ///     Adds the sideEffect step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > SideEffect (params object[] args)
        {
            Bytecode.AddStep("sideEffect", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the simplePath step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > SimplePath (params object[] args)
        {
            Bytecode.AddStep("simplePath", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the store step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Store (params object[] args)
        {
            Bytecode.AddStep("store", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the subgraph step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Edge > Subgraph (params object[] args)
        {
            Bytecode.AddStep("subgraph", args);
            return Wrap< S , Edge >(this);
        }

        /// <summary>
        ///     Adds the sum step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Sum<E2> (params object[] args)
        {
            Bytecode.AddStep("sum", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the tail step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Tail<E2> (params object[] args)
        {
            Bytecode.AddStep("tail", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the timeLimit step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > TimeLimit (params object[] args)
        {
            Bytecode.AddStep("timeLimit", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the times step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Times (params object[] args)
        {
            Bytecode.AddStep("times", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the to step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > To (params object[] args)
        {
            Bytecode.AddStep("to", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the toE step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Edge > ToE (params object[] args)
        {
            Bytecode.AddStep("toE", args);
            return Wrap< S , Edge >(this);
        }

        /// <summary>
        ///     Adds the toV step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , Vertex > ToV (params object[] args)
        {
            Bytecode.AddStep("toV", args);
            return Wrap< S , Vertex >(this);
        }

        /// <summary>
        ///     Adds the tree step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Tree (params object[] args)
        {
            Bytecode.AddStep("tree", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the unfold step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Unfold<E2> (params object[] args)
        {
            Bytecode.AddStep("unfold", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the union step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Union<E2> (params object[] args)
        {
            Bytecode.AddStep("union", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the until step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Until (params object[] args)
        {
            Bytecode.AddStep("until", args);
            return Wrap< S , E >(this);
        }

        /// <summary>
        ///     Adds the value step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Value<E2> (params object[] args)
        {
            Bytecode.AddStep("value", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the valueMap step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , IDictionary<string, E2> > ValueMap<E2> (params object[] args)
        {
            Bytecode.AddStep("valueMap", args);
            return Wrap< S , IDictionary<string, E2> >(this);
        }

        /// <summary>
        ///     Adds the values step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E2 > Values<E2> (params object[] args)
        {
            Bytecode.AddStep("values", args);
            return Wrap< S , E2 >(this);
        }

        /// <summary>
        ///     Adds the where step to this <see cref="GraphTraversal{SType, EType}" />.
        /// </summary>
        public GraphTraversal< S , E > Where (params object[] args)
        {
            Bytecode.AddStep("where", args);
            return Wrap< S , E >(this);
        }

    }
}