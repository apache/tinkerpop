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

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;

public final class SymbolHelper {

    private static final Map<String, String> TO_CSHARP_TYPE_MAP = new HashMap<>();

    static {
        TO_CSHARP_TYPE_MAP.put("Long", "long");
        TO_CSHARP_TYPE_MAP.put("Integer", "int");
        TO_CSHARP_TYPE_MAP.put("String", "string");
        TO_CSHARP_TYPE_MAP.put("Object", "object");
        TO_CSHARP_TYPE_MAP.put("java.util.Map<java.lang.String, E2>", "IDictionary<string, E2>");
        TO_CSHARP_TYPE_MAP.put("java.util.Map<java.lang.String, B>", "IDictionary<string, E2>")
        TO_CSHARP_TYPE_MAP.put("java.util.List<E>", "IList<E>");
        TO_CSHARP_TYPE_MAP.put("java.util.List<A>", "IList<object>");
        TO_CSHARP_TYPE_MAP.put("java.util.Map<K, V>", "IDictionary<K, V>");
        TO_CSHARP_TYPE_MAP.put("java.util.Collection<E2>", "ICollection<E2>");
        TO_CSHARP_TYPE_MAP.put("java.util.Collection<B>", "ICollection<E2>")
        TO_CSHARP_TYPE_MAP.put("java.util.Map<K, java.lang.Long>", "IDictionary<K, long>");
        TO_CSHARP_TYPE_MAP.put("TraversalMetrics", "E2");
    }

    public static String toCSharp(final String symbol) {
        return (String) Character.toUpperCase(symbol.charAt(0)) + symbol.substring(1)
    }

    public static String toJava(final String symbol) {
        return (String) Character.toLowerCase(symbol.charAt(0)) + symbol.substring(1)
    }

    public static String toCSharpType(final String name) {
        String typeName = TO_CSHARP_TYPE_MAP.getOrDefault(name, name);
        if (typeName.equals(name) && (typeName.contains("? extends") || typeName.equals("Tree"))) {
            typeName = "E2";
        }
        return typeName;
    }

    public static String[] getJavaParameterTypeNames(final Method method) {
        def typeArguments = ((ParameterizedType)method.genericReturnType).actualTypeArguments;
        return typeArguments.
                collect { (it instanceof Class) ? ((Class)it).simpleName : it.typeName }.
                collect { name ->
                    if (name.equals("A")) {
                        name = "object";
                    }
                    else if (name.equals("B")) {
                        name = "E2";
                    }
                    name;
                };
    }

    public static String getCSharpGenericTypeParam(String typeName) {
        def tParam = "";
        if (typeName.contains("E2")) {
            tParam = "<E2>";
        }
        else if (typeName.contains("<K, V>")) {
            tParam = "<K, V>";
        }
        else if (typeName.contains("<K, ")) {
            tParam = "<K>";
        }
        return tParam;
    }
}
