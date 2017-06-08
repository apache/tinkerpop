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

class GraphTraversalGenerator {

    public static void create(final String graphTraversalFile) {

        final StringBuilder csharpClass = new StringBuilder()

        csharpClass.append(CommonContentHelper.getLicense())

        csharpClass.append(
"""
using System.Collections.Generic;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.CSharp.Process
{
    public class GraphTraversal : DefaultTraversal
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
""")
        GraphTraversal.getMethods().
                findAll { GraphTraversal.class.equals(it.returnType) }.
                findAll { !it.name.equals("clone") && !it.name.equals("iterate") }.
                collect { it.name }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { javaMethodName ->
                    String sharpMethodName = SymbolHelper.toCSharp(javaMethodName)

                    csharpClass.append(
                            """
        public GraphTraversal ${sharpMethodName}(params object[] args)
        {
            Bytecode.AddStep("${javaMethodName}", args);
            return this;
        }
""")
                }
        csharpClass.append("\t}\n")
        csharpClass.append("}")

        final File file = new File(graphTraversalFile);
        file.delete()
        csharpClass.eachLine { file.append(it + "\n") }
    }
}
