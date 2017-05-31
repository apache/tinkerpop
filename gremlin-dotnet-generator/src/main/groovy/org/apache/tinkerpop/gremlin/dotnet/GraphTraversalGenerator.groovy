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

package org.apache.tinkerpop.gremlin.dotnet

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

class GraphTraversalGenerator {

    public static void create(final String graphTraversalFile) {

        final StringBuilder csharpClass = new StringBuilder()

        csharpClass.append(CommonContentHelper.getLicense())

        csharpClass.append(
"""
using System.Collections.Generic;
using Gremlin.Net.Structure;

namespace Gremlin.Net.Process.Traversal
{
    public class GraphTraversal<S, E> : DefaultTraversal<S, E>
    {
        public GraphTraversal()
            : this(new List<ITraversalStrategy>(), new Bytecode())
        {
        }

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

""")
        GraphTraversal.getMethods().
                findAll { GraphTraversal.class.equals(it.returnType) }.
                findAll { !it.name.equals("clone") && !it.name.equals("iterate") }.
                groupBy { it.name }.
                // Select unique by name, with the most amount of parameters
                collect { it.value.sort { a, b -> b.parameterCount <=> a.parameterCount }.first() }.
                sort { it.name }.
                forEach { javaMethod ->
                    String[] typeNames = SymbolHelper.getJavaParameterTypeNames(javaMethod);
                    def t1 = SymbolHelper.toCSharpType(typeNames[0]);
                    def t2 = SymbolHelper.toCSharpType(typeNames[1]);
                    def tParam = SymbolHelper.getCSharpGenericTypeParam(t2);
                    csharpClass.append(
"""
        public GraphTraversal<$t1, $t2> ${SymbolHelper.toCSharp(javaMethod.name)}$tParam(params object[] args)
        {
            Bytecode.AddStep("$javaMethod.name", args);
            return Wrap<$t1, $t2>(this);
        }
""")
                }
        csharpClass.append("    }\n}")

        final File file = new File(graphTraversalFile);
        file.delete()
        csharpClass.eachLine { file.append(it + "\n") }
    }
}
