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
package org.apache.tinkerpop.machine.structure.data;

import org.apache.tinkerpop.machine.util.IteratorUtils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JMap<K, V> implements TMap<K, V> {

    private final Map<K, V> map;

    public JMap() {
        this.map = new LinkedHashMap<>();
    }

    public JMap(final Map<K, V> map) {
        this.map = map;
    }

    @Override
    public void set(final K key, final V value) {
        this.map.put(key, value);
    }

    @Override
    public V get(final K key) {
        return this.map.get(key);
    }

    @Override
    public V get(final K key, final V defaultValue) {
        return this.map.getOrDefault(key, defaultValue);
    }

    @Override
    public boolean has(final K key) {
        return this.map.containsKey(key);
    }

    @Override
    public Iterator<K> keys() {
        return this.map.keySet().iterator();
    }

    @Override
    public Iterator<V> values() {
        return this.map.values().iterator();
    }

    @Override
    public Iterator<TTuple2<K, V>> entries() {
        return IteratorUtils.map(this.map.entrySet().iterator(),
                entry -> new JTuple2<>(entry.getKey(), entry.getValue()));
    }

    @Override
    public int hashCode() {
        return this.map.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof JMap && this.map.equals(((JMap) object).map);
    }

    @Override
    public String toString() {
        return this.map.toString();
    }

    public static <K, V> JMap<K, V> create(final Map<K, V> map) {
        return new JMap<>(map);
    }
}
