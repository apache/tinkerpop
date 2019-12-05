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
 * @deprecated As of release 3.3.10, not replaced - see TINKERPOP-2317
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Deprecated
public final class SymbolHelper {

    private final static Map<String, String> TO_PYTHON_MAP = new HashMap<>();
    private final static Map<String, String> FROM_PYTHON_MAP = new HashMap<>();

    static {
        TO_PYTHON_MAP.put("global", "global_");
        TO_PYTHON_MAP.put("as", "as_");
        TO_PYTHON_MAP.put("in", "in_");
        TO_PYTHON_MAP.put("and", "and_");
        TO_PYTHON_MAP.put("or", "or_");
        TO_PYTHON_MAP.put("is", "is_");
        TO_PYTHON_MAP.put("not", "not_");
        TO_PYTHON_MAP.put("from", "from_");
        TO_PYTHON_MAP.put("list", "list_");
        TO_PYTHON_MAP.put("set", "set_");
        TO_PYTHON_MAP.put("all", "all_");
        TO_PYTHON_MAP.put("with", "with_");
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
