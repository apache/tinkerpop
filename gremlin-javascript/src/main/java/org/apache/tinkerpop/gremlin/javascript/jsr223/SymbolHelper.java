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

package org.apache.tinkerpop.gremlin.javascript.jsr223;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jorge Bay Gondra
 */
public final class SymbolHelper {

    private final static Map<String, String> TO_JS_MAP = new HashMap<>();
    private final static Map<String, String> FROM_JS_MAP = new HashMap<>();

    static {
        TO_JS_MAP.put("in", "in_");
        TO_JS_MAP.put("from", "from_");
        TO_JS_MAP.forEach((k, v) -> FROM_JS_MAP.put(v, k));
    }

    private SymbolHelper() {
        // static methods only, do not instantiate
    }

    public static String toJs(final String symbol) {
        return TO_JS_MAP.getOrDefault(symbol, symbol);
    }

    public static String toJava(final String symbol) {
        return FROM_JS_MAP.getOrDefault(symbol, symbol);
    }

    public static String decapitalize(String string) {
        if (string == null || string.length() == 0) {
            return string;
        }
        char c[] = string.toCharArray();
        c[0] = Character.toLowerCase(c[0]);
        return new String(c);
    }
}
