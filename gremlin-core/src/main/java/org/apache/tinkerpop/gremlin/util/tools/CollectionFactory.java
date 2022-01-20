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
package org.apache.tinkerpop.gremlin.util.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

public class CollectionFactory {

    public static <E> List<E> asList(final E... elements) {
        return new ArrayList<>(Arrays.asList(elements));
    }

    public static <E> LinkedHashSet<E> asSet(final E... elements) {
        return asSet(Arrays.asList(elements));
    }

    public static <E> LinkedHashSet<E> asSet(final Collection<E> elements) {
        return new LinkedHashSet<>(elements);
    }

    public static <K,V> LinkedHashMap<K,V> asMap(final Object... elements) {
        final LinkedHashMap<K,V> map = new LinkedHashMap<>();
        for (int i = 0; i < elements.length; i+=2) {
            final K k = (K) elements[i];
            final V v = (V) (i+1 < elements.length ? elements[i+1] : null);
            map.put(k, v);
        }
        return map;
    }

}
