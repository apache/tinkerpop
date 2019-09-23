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
package org.apache.tinkerpop.gremlin.process.traversal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * General representation of script
 *
 * @author Stark Arya (sandszhou.zj@alibaba-inc.com)
 */
public final class Script {
    private static final String KEY_PREFIX = "_args_";
    private final StringBuilder  scriptBuilder;
    private final Map<Object, String> parameters;

    public Script() {
        scriptBuilder = new StringBuilder();
        parameters = new HashMap<>();
    }

    public void init() {
        scriptBuilder.setLength(0);
        parameters.clear();
    }

    public Script append(final String script) {
       scriptBuilder.append(script);
       return this;
    }

    public Script setCharAtEnd(final char ch) {
        scriptBuilder.setCharAt(scriptBuilder.length() - 1, ch);
        return this;
    }

    public <V> Script getBoundKeyOrAssign(final boolean withParameters, final V value) {
        if (withParameters) {
            if (!parameters.containsKey(value)) {
                parameters.put(value, getNextBoundKey());
            }
            append(parameters.get(value));
        } else {
            append(value.toString());
        }
        return this;
    }

    public String getScript() {
        return scriptBuilder.toString();
    }

    public Optional<Map<String,Object>> getParameters() {
        return Optional.ofNullable(parameters.isEmpty() ? null : parameters.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)));
    }

    /**
     * @return  a monotonically increasing key
     */
    private String getNextBoundKey() {
        return KEY_PREFIX + parameters.size();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        final List<String> strings = Stream.of(new Object[]{getScript(), getParameters()})
                .filter(o -> null != o)
                .filter(o -> {
                    if (o instanceof Map) {
                        return !((Map) o).isEmpty();
                    } else {
                        return !o.toString().isEmpty();
                    }
                })
                .map(Object::toString).collect(Collectors.toList());
        if (!strings.isEmpty()) {
            builder.append('(');
            builder.append(String.join(",", strings));
            builder.append(')');
        }
        return "Script[" + builder.toString() + "]";
    }
}