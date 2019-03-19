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
package org.apache.tinkerpop.machine.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A number of static methods to interact with a multi map, i.e. a map that maps keys to sets of values.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public final class MultiMap {

    private MultiMap() {
    }

    public static <K, V> boolean putAll(final Map<K, Set<V>> map, final K key, final Collection<V> values) {
        return getMapSet(map, key).addAll(values);
    }

    public static <K, V> boolean put(final Map<K, Set<V>> map, final K key, final V value) {
        return getMapSet(map, key).add(value);
    }

    public static <K, V> boolean containsEntry(final Map<K, Set<V>> map, final K key, final V value) {
        final Set<V> set = map.get(key);
        return set != null && set.contains(value);
    }

    public static <K, V> Set<V> get(final Map<K, Set<V>> map, final K key) {
        final Set<V> set = getMapSet(map, key);
        return set == null ? Collections.emptySet() : set;
    }

    private static <K, V> Set<V> getMapSet(final Map<K, Set<V>> map, final K key) {
        Set<V> set = map.get(key);
        if (set == null) {
            set = new LinkedHashSet<>();
            map.put(key, set);
        }
        return set;
    }


}
