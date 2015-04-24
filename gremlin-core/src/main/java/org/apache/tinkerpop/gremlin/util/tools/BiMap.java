/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.util.tools;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BiMap<K, V> implements Serializable {

    private Map<K, V> leftMap = new HashMap<>();
    private Map<V, K> rightMap = new HashMap<>();

    public int size() {
        return this.leftMap.size();
    }

    public boolean isEmpty() {
        return this.leftMap.isEmpty();
    }

    public boolean containsKey(final K key) {
        return this.leftMap.containsKey(key);
    }

    public boolean containsValue(final V value) {
        return this.rightMap.containsKey(value);
    }

    public V getValue(final K key) {
        return this.leftMap.get(key);
    }

    public K getKey(final V value) {
        return this.rightMap.get(value);
    }

    public V put(final K key, final V value) {
        this.rightMap.put(value, key);
        return this.leftMap.put(key, value);
    }

    public V removeKey(final K key) {
        final V v = this.leftMap.remove(key);
        if (null != v)
            this.rightMap.remove(v);
        return v;
    }

    public K removeValue(final V value) {
        final K k = this.rightMap.remove(value);
        if (null != k)
            this.leftMap.remove(k);
        return k;
    }

    public void clear() {
        this.leftMap.clear();
        this.rightMap.clear();
    }

    public Set<K> keySet() {
        return this.leftMap.keySet();
    }

    public Set<V> valueSet() {
        return this.rightMap.keySet();
    }
}
