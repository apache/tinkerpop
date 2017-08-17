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
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Gremlin.Net.Structure;

// THIS IS A GENERATED FILE - DO NOT MODIFY THIS FILE DIRECTLY - see pom.xml
namespace Gremlin.Net.Process.Traversal
{
#pragma warning disable 1591

    /// <summary>
    ///     A <see cref="GraphTraversalSource" /> is the primary DSL of the Gremlin traversal machine.
    ///     It provides access to all the configurations and steps for Turing complete graph computing.
    /// </summary>
    public class GraphTraversalSource
    {
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

        public GraphTraversalSource WithSack(object initialValue)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue);
            return source;
        }

        public GraphTraversalSource WithSack(object initialValue, object mergeOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue, mergeOperator);
            return source;
        }

        public GraphTraversalSource WithSack(object initialValue, object splitOperator, object mergeOperator)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSack", initialValue, splitOperator, mergeOperator);
            return source;
        }

        public GraphTraversalSource WithSideEffect(string key, object initialValue)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSideEffect", key, initialValue);
            return source;
        }

        public GraphTraversalSource WithSideEffect(string key, object initialValue, object reducer)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            source.Bytecode.AddSource("withSideEffect", key, initialValue, reducer);
            return source;
        }

        public GraphTraversalSource WithStrategies(params ITraversalStrategy[] traversalStrategies)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            var args = new List<object> {};
            args.AddRange(traversalStrategies);
            source.Bytecode.AddSource("withStrategies", args.ToArray());
            return source;
        }

        public GraphTraversalSource WithoutStrategies(params Type[] traversalStrategyClasses)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                                                  new Bytecode(Bytecode));
            var args = new List<object> {};
            args.AddRange(traversalStrategyClasses);
            source.Bytecode.AddSource("withoutStrategies", args.ToArray());
            return source;
        }

        [Obsolete("Use the Bindings class instead.", false)]
        public GraphTraversalSource WithBindings(object bindings)
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
        public GraphTraversalSource WithRemote(IRemoteConnection remoteConnection)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                new Bytecode(Bytecode));
            source.TraversalStrategies.Add(new RemoteStrategy(remoteConnection));
            return source;
        }

        /// <summary>
        ///     Add a GraphComputer class used to execute the traversal.
        ///     This adds a <see cref="VertexProgramStrategy" /> to the strategies.
        /// </summary>
        public GraphTraversalSource WithComputer(string graphComputer = null, int? workers = null, string persist = null,
            string result = null, ITraversal vertices = null, ITraversal edges = null,
            Dictionary<string, dynamic> configuration = null)
        {
            return WithStrategies(new VertexProgramStrategy(graphComputer, workers, persist, result, vertices, edges, configuration));
        }


        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the E step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal< Edge,Edge > E(params object[] edgesIds)
        {
            var traversal = new GraphTraversal< Edge,Edge >(TraversalStrategies, new Bytecode(Bytecode));
            var args = new List<object> {};
            args.AddRange(edgesIds);
            traversal.Bytecode.AddStep("E", args.ToArray());
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the V step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal< Vertex,Vertex > V(params object[] vertexIds)
        {
            var traversal = new GraphTraversal< Vertex,Vertex >(TraversalStrategies, new Bytecode(Bytecode));
            var args = new List<object> {};
            args.AddRange(vertexIds);
            traversal.Bytecode.AddStep("V", args.ToArray());
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal< Vertex,Vertex > AddV()
        {
            var traversal = new GraphTraversal< Vertex,Vertex >(TraversalStrategies, new Bytecode(Bytecode));
                traversal.Bytecode.AddStep("addV");
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal< Vertex,Vertex > AddV(params object[] keyValues)
        {
            var traversal = new GraphTraversal< Vertex,Vertex >(TraversalStrategies, new Bytecode(Bytecode));
            var args = new List<object> {};
            args.AddRange(keyValues);
            traversal.Bytecode.AddStep("addV", args.ToArray());
            return traversal;
        }

        /// <summary>
        ///     Spawns a <see cref="GraphTraversal{SType, EType}" /> off this graph traversal source and adds the addV step to that
        ///     traversal.
        /// </summary>
        public GraphTraversal< Vertex,Vertex > AddV(string label)
        {
            var traversal = new GraphTraversal< Vertex,Vertex >(TraversalStrategies, new Bytecode(Bytecode));
                traversal.Bytecode.AddStep("addV", label);
            return traversal;
        }

    }
    
#pragma warning restore 1591
}