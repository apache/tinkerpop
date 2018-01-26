/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.sparql;

import java.util.Arrays;
import java.util.List;

/**
 * Helper methods for working with prefix lines in SPARQL queries.
 */
class Prefixes {

    final static String BASE_URI = "http://tinkerpop.apache.org/traversal/";

    private final static List<String> PREFIXES = Arrays.asList("edge", "property", "value");

    private final static String PREFIX_DEFINITIONS;

    static {
        final StringBuilder builder = new StringBuilder();
        for (final String prefix : PREFIXES) {
            builder.append("PREFIX ").append(prefix.substring(0, 1)).append(": <").append(getURI(prefix)).
                    append(">").append(System.lineSeparator());
        }
        PREFIX_DEFINITIONS = builder.toString();
    }

    static String getURI(final String prefix) {
        return BASE_URI + prefix + "#";
    }

    static String getURIValue(final String uri) {
        return uri.substring(uri.indexOf("#") + 1);
    }

    static String getPrefix(final String uri) {
        final String tmp = uri.substring(0, uri.indexOf("#"));
        return tmp.substring(tmp.lastIndexOf("/") + 1);
    }

    static String prepend(final String script) {
        return PREFIX_DEFINITIONS + script;
    }

    static StringBuilder prepend(final StringBuilder scriptBuilder) {
        return scriptBuilder.insert(0, PREFIX_DEFINITIONS);
    }
}
