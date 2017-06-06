/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.csharp

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource


class GraphTraversalSourceGenerator {

    public static void create(final String graphTraversalSourceFile) {

        final StringBuilder csharpClass = new StringBuilder()

        csharpClass.append(CommonContentHelper.getLicense())

        csharpClass.append(
"""
using System.Collections.Generic;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;

namespace Gremlin.Net.Process.Traversal
{
    public class GraphTraversalSource
    {
        public ICollection<ITraversalStrategy> TraversalStrategies { get; set; }
        public Bytecode Bytecode { get; set; }

         public GraphTraversalSource()
            : this(new List<ITraversalStrategy>(), new Bytecode())
        {
        }

        public GraphTraversalSource(ICollection<ITraversalStrategy> traversalStrategies, Bytecode bytecode)
        {
            TraversalStrategies = traversalStrategies;
            Bytecode = bytecode;
        }
"""
        )

        // Hold the list of methods with their overloads, so we do not create duplicates
        HashMap<String, ArrayList<String>> sharpMethods = new HashMap<String, ArrayList<String>>()

        GraphTraversalSource.getMethods(). // SOURCE STEPS
                findAll { GraphTraversalSource.class.equals(it.returnType) }.
                findAll {
                    !it.name.equals("clone") &&
                            // replace by TraversalSource.Symbols.XXX
                            !it.name.equals("withBindings") &&
                            !it.name.equals("withRemote") &&
                            !it.name.equals("withComputer")
                }.
                collect { it.name }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { javaMethodName ->
                    String sharpMethodName = SymbolHelper.toCSharp(javaMethodName)

                    csharpClass.append(
"""
        public GraphTraversalSource ${sharpMethodName}(params object[] args)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                new Bytecode(Bytecode));
            source.Bytecode.AddSource("${javaMethodName}\", args);
            return source;
        }
""")
                }

        csharpClass.append(
                """
        public GraphTraversalSource WithBindings(object bindings)
        {
            return this;
        }

        public GraphTraversalSource WithRemote(IRemoteConnection remoteConnection)
        {
            var source = new GraphTraversalSource(new List<ITraversalStrategy>(TraversalStrategies),
                new Bytecode(Bytecode));
            source.TraversalStrategies.Add(new RemoteStrategy(remoteConnection));
            return source;
        }
        
        public GraphTraversalSource WithComputer(string graphComputer = null, int? workers = null, string persist = null,
            string result = null, ITraversal vertices = null, ITraversal edges = null,
            Dictionary<string, dynamic> configuration = null)
        {
            return WithStrategies(new VertexProgramStrategy(graphComputer, workers, persist, result, vertices, edges, configuration));
        }
""")

        GraphTraversalSource.getMethods(). // SPAWN STEPS
                findAll { GraphTraversal.class.equals(it.returnType) }.
                collect { it.name }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { javaMethodName ->
                    String sharpMethodName = SymbolHelper.toCSharp(javaMethodName)

                    csharpClass.append(
                            """
        public GraphTraversal ${sharpMethodName}(params object[] args)
        {
            var traversal = new GraphTraversal(TraversalStrategies, new Bytecode(Bytecode));
            traversal.Bytecode.AddStep("${javaMethodName}\", args);
            return traversal;
        }
""")
                }

        csharpClass.append("\t}\n")
        csharpClass.append("}")

        final File file = new File(graphTraversalSourceFile);
        file.delete()
        csharpClass.eachLine { file.append(it + "\n") }
    }
}