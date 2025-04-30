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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TinkerIndex<T extends Element> extends AbstractTinkerIndex<T> {

    protected Map<String, Map<Object, Set<T>>> index = new ConcurrentHashMap<>();

    public TinkerIndex(final TinkerGraph graph, final Class<T> indexClass) {
        super(graph, indexClass);
    }

    protected void put(final String key, final Object value, final T element) {
        Map<Object, Set<T>> keyMap = this.index.get(key);
        if (null == keyMap) {
            this.index.putIfAbsent(key, new ConcurrentHashMap<>());
            keyMap = this.index.get(key);
        }
        final Object indexableValue = indexable(value);
        Set<T> objects = keyMap.get(indexableValue);
        if (null == objects) {
            keyMap.putIfAbsent(indexableValue, ConcurrentHashMap.newKeySet());
            objects = keyMap.get(indexableValue);
        }
        objects.add(element);
    }

    @Override
    public List<T> get(final String key, final Object value) {
        final Map<Object, Set<T>> keyMap = this.index.get(key);
        if (null == keyMap) {
            return Collections.emptyList();
        } else {
            Set<T> set = keyMap.get(indexable(value));
            if (null == set)
                return Collections.emptyList();
            else
                return new ArrayList<>(set);
        }
    }

    @Override
    public long count(final String key, final Object value) {
        final Map<Object, Set<T>> keyMap = this.index.get(key);
        if (null == keyMap) {
            return 0;
        } else {
            final Set<T> set = keyMap.get(indexable(value));
            if (null == set)
                return 0;
            else
                return set.size();
        }
    }

    @Override
    public void remove(final String key, final Object value, final T element) {
        final Map<Object, Set<T>> keyMap = this.index.get(key);
        if (null != keyMap) {
            final Set<T> objects = keyMap.get(indexable(value));
            if (null != objects) {
                objects.remove(element);
                if (objects.size() == 0) {
                    keyMap.remove(value);
                }
            }
        }
    }

    @Override
    public void removeElement(final T element) {
        if (this.indexClass.isAssignableFrom(element.getClass())) {
            for (Map<Object, Set<T>> map : index.values()) {
                for (Set<T> set : map.values()) {
                    set.remove(element);
                }
            }
        }
    }

    @Override
    public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
        if (this.indexedKeys.contains(key)) {
            this.remove(key, oldValue, element);
            this.put(key, newValue, element);
        }
    }

    @Override
    public void createIndex(final String key, final Map<String,Object> configuration) {
        if (null == key)
            throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (key.isEmpty())
            throw new IllegalArgumentException("The key for the index cannot be an empty string");

        if (this.indexedKeys.contains(key))
            return;
        this.indexedKeys.add(key);

        (Vertex.class.isAssignableFrom(this.indexClass) ?
                // cleaner to use graph.vertices(), but graph.vertices is quicker
                ((TinkerGraph)this.graph).vertices.values().parallelStream() :
                ((TinkerGraph)this.graph).edges.values().parallelStream())
                .map(e -> new Object[]{((T) e).property(key), e})
                .filter(a -> ((Property) a[0]).isPresent())
                .forEach(a -> this.put(key, ((Property) a[0]).value(), (T) a[1]));
    }

    @Override
    public void dropIndex(final String key) {
        if (this.index.containsKey(key))
            this.index.remove(key).clear();

        this.indexedKeys.remove(key);
    }
}
