/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.match;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Bindings<T> {
    private final SortedMap<String, T> map = new TreeMap<>();

    public Bindings() {
    }

    public Bindings(final Map<String, T> map) {
        this.map.putAll(map);
    }

    public Bindings<T> put(final String name, final T value) {
        map.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, T> entry : map.entrySet()) {
            if (first) first = false;
            else sb.append(", ");
            sb.append(entry.getKey()).append(':').append(entry.getValue());
        }
        sb.append('}');
        return sb.toString();
    }

    public static class BindingsComparator<T> implements Comparator<Bindings<T>> {
        private final Function<T, String> toStringFunction;

        public BindingsComparator(Function<T, String> toStringFunction) {
            this.toStringFunction = toStringFunction;
        }

        @Override
        public int compare(Bindings<T> left, Bindings<T> right) {
            int cmp = ((Integer) left.map.size()).compareTo(right.map.size());
            if (0 != cmp) return cmp;

            Iterator<Map.Entry<String, T>> i1 = left.map.entrySet().iterator();
            Iterator<Map.Entry<String, T>> i2 = right.map.entrySet().iterator();
            while (i1.hasNext()) {
                Map.Entry<String, T> e1 = i1.next();
                Map.Entry<String, T> e2 = i2.next();

                cmp = e1.getKey().compareTo(e1.getKey());
                if (0 != cmp) return cmp;

                cmp = e1.getValue().toString().compareTo(e2.getValue().toString());
                if (0 != cmp) return cmp;
            }

            return 0;
        }
    }
}
