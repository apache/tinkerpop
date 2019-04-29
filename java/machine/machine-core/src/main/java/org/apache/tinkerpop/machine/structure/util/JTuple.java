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
package org.apache.tinkerpop.machine.structure.util;

import org.apache.tinkerpop.machine.structure.TSequence;
import org.apache.tinkerpop.machine.structure.TTuple;
import org.apache.tinkerpop.machine.util.IteratorUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JTuple<K, V> implements TTuple<K, V> {

    private final Map<K, V> map = new HashMap<>();

    @Override
    public boolean has(final K key) {
        return this.map.containsKey(key);
    }

    @Override
    public V value(final K key) {
        return this.map.get(key);
    }

    @Override
    public void set(final K key, final V value) {
        this.map.put(key, value);
    }

    @Override
    public TSequence<T2Tuple<K, V>> entries() {
        return () -> IteratorUtils.map(this.map.entrySet().iterator(), e -> new J2Tuple<>(e.getKey(), e.getValue()));
    }

}
