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
import java.util.Map;

/**
 * Bindings are used to associate a variable with a value.
 * They enable the creation of {@link org.apache.tinkerpop.gremlin.process.traversal.Bytecode.Binding} arguments in {@link Bytecode}.
 * Use the Bindings instance when defining a binding via {@link Bindings#of(String, Object)}.
 * For instance:
 * <p>
 * <code>
 * b = Bindings.instance()
 * g = graph.traversal()
 * g.V().out(b.of("a","knows"))
 * // bindings can be reused over and over
 * g.V().out("knows").in(b.of("a","created"))
 * </code>
 * </p>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Bindings {

    private static final Bindings INSTANCE = new Bindings();
    private static final ThreadLocal<Map<Object, String>> MAP = new ThreadLocal<>();

    /**
     * @deprecated As of release 3.2.4, replaced by {@link Bindings#instance()}.
     */
    @Deprecated
    public Bindings() {

    }

    public <V> V of(final String variable, final V value) {
        Map<Object, String> map = MAP.get();
        if (null == map) {
            map = new HashMap<>();
            MAP.set(map);
        }
        map.put(value, variable);
        return value;
    }

    protected static <V> String getBoundVariable(final V value) {
        final Map<Object, String> map = MAP.get();
        return null == map ? null : map.get(value);
    }

    protected static void clear() {
        final Map<Object, String> map = MAP.get();
        if (null != map)
            map.clear();
    }

    public static Bindings instance() {
        return INSTANCE;
    }

    @Override
    public String toString() {
        return "bindings[" + Thread.currentThread().getName() + "]";
    }
}
