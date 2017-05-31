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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__

import java.lang.reflect.Modifier

class AnonymousTraversalGenerator {

    private static final Map<String, String[]> METHODS_WITH_SPECIFIC_TYPES = new HashMap<>();

    static {
        String[] useE2 = ["E2", "E2"];
        METHODS_WITH_SPECIFIC_TYPES.put("constant", useE2);
        METHODS_WITH_SPECIFIC_TYPES.put("limit", useE2);
        METHODS_WITH_SPECIFIC_TYPES.put("mean", useE2);
        METHODS_WITH_SPECIFIC_TYPES.put("optional", useE2);
        METHODS_WITH_SPECIFIC_TYPES.put("range", useE2);
        METHODS_WITH_SPECIFIC_TYPES.put("select", ["IDictionary<string, E2>", "E2"] as String[]);
        METHODS_WITH_SPECIFIC_TYPES.put("sum", useE2);
        METHODS_WITH_SPECIFIC_TYPES.put("tail", useE2);
        METHODS_WITH_SPECIFIC_TYPES.put("tree", ["object"] as String[]);
        METHODS_WITH_SPECIFIC_TYPES.put("unfold", useE2);
    }

    public static void create(final String anonymousTraversalFile) {


        final StringBuilder csharpClass = new StringBuilder()

        csharpClass.append(CommonContentHelper.getLicense())

        csharpClass.append(
"""
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
""")
        __.getMethods().
                findAll { GraphTraversal.class.equals(it.returnType) }.
                findAll { Modifier.isStatic(it.getModifiers()) }.
                findAll { !it.name.equals("__") && !it.name.equals("start") }.
                groupBy { it.name }.
                // Select unique by name, with the most amount of parameters
                collect { it.value.sort { a, b -> b.parameterCount <=> a.parameterCount }.first() }.
                sort { it.name }.
                forEach { javaMethod ->
                    String sharpMethodName = SymbolHelper.toCSharp(javaMethod.name);
                    String[] typeNames = SymbolHelper.getJavaParameterTypeNames(javaMethod);
                    def t2 = SymbolHelper.toCSharpType(typeNames[1]);
                    def tParam = SymbolHelper.getCSharpGenericTypeParam(t2);
                    def specificTypes = METHODS_WITH_SPECIFIC_TYPES.get(javaMethod.name);
                    if (specificTypes) {
                        t2 = specificTypes[0];
                        tParam = specificTypes.length > 1 ? "<" + specificTypes[1] + ">" : "";
                    }
                    csharpClass.append(
"""
        public static GraphTraversal<object, $t2> $sharpMethodName$tParam(params object[] args)
        {
            return new GraphTraversal<object, object>().$sharpMethodName$tParam(args);
        }
""")
                }
        csharpClass.append("    }\n}")

        final File file = new File(anonymousTraversalFile);
        file.delete()
        csharpClass.eachLine { file.append(it + "\n") }
    }
}
