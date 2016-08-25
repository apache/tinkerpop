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

package org.apache.tinkerpop.gremlin.python.jsr223;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SymbolHelper {

    private final static Map<String, String> TO_PYTHON_MAP = new HashMap<>();
    private final static Map<String, String> FROM_PYTHON_MAP = new HashMap<>();

    static {
        TO_PYTHON_MAP.put("global", "_global");
        TO_PYTHON_MAP.put("as", "_as");
        TO_PYTHON_MAP.put("in", "_in");
        TO_PYTHON_MAP.put("and", "_and");
        TO_PYTHON_MAP.put("or", "_or");
        TO_PYTHON_MAP.put("is", "_is");
        TO_PYTHON_MAP.put("not", "_not");
        TO_PYTHON_MAP.put("from", "_from");
        TO_PYTHON_MAP.put("list", "_list");
        TO_PYTHON_MAP.put("set", "_set");
        TO_PYTHON_MAP.put("all", "_all");
        //
        TO_PYTHON_MAP.forEach((k, v) -> FROM_PYTHON_MAP.put(v, k));
    }

    private SymbolHelper() {
        // static methods only, do not instantiate
    }

    public static String toPython(final String symbol) {
        return TO_PYTHON_MAP.getOrDefault(symbol, symbol);
    }

    public static String toJava(final String symbol) {
        return FROM_PYTHON_MAP.getOrDefault(symbol, symbol);
    }

}
