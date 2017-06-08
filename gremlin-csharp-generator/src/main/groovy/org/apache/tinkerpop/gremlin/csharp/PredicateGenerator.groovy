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

import org.apache.tinkerpop.gremlin.process.traversal.P

import java.lang.reflect.Modifier

class PredicateGenerator {

    public static void create(final String predicateFile) {

        final StringBuilder csharpClass = new StringBuilder()

        csharpClass.append(CommonContentHelper.getLicense())

        csharpClass.append(
"""
using Gremlin.Net.Process.Traversal;

namespace Gremlin.CSharp.Process
{
    public class P
    {""")
        P.class.getMethods().
                findAll { Modifier.isStatic(it.getModifiers()) }.
                findAll { P.class.isAssignableFrom(it.returnType) }.
                collect { it.name }.
                unique().
                sort { a, b -> a <=> b }.
                each { javaMethodName ->
                    String sharpMethodName = SymbolHelper.toCSharp(javaMethodName)
                    csharpClass.append(
"""
        public static TraversalPredicate ${sharpMethodName}(params object[] args)
        {
            var value = args.Length == 1 ? args[0] : args;
            return new TraversalPredicate("${javaMethodName}", value);
        }
""")
                }
        csharpClass.append("\t}\n")
        csharpClass.append("}")

        final File file = new File(predicateFile)
        file.delete()
        csharpClass.eachLine { file.append(it + "\n") }
    }
}
