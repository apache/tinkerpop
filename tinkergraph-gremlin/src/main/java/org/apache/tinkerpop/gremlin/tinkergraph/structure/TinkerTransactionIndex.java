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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Valentyn Kahamlyk
 */
final class TinkerTransactionIndex<T extends TinkerElement> extends AbstractTinkerIndex<T> {

    protected Map<String, Map<Object, Set<TinkerElementContainer<T>>>> index = new ConcurrentHashMap<>();
    protected ThreadLocal<Map<String, Map<Object, Set<T>>>> txIndex =
            ThreadLocal.withInitial(() -> new ConcurrentHashMap<>());

    public TinkerTransactionIndex(final TinkerTransactionGraph graph, final Class<T> indexClass) {
        super(graph, indexClass);
    }

    private void putTxElement(final String key, final Object value, final T element) {
        Map<String, Map<Object, Set<T>>> index = txIndex.get();
        if (index == null) {
            txIndex.set(new HashMap<>());
            index = txIndex.get();
        }

        Map<Object, Set<T>> keyMap = index.get(key);
        if (null == keyMap) {
            index.putIfAbsent(key, new ConcurrentHashMap<>());
            keyMap = index.get(key);
        }
        final Object indexableValue = indexable(value);
        Set<T> objects = keyMap.get(indexableValue);
        if (null == objects) {
            keyMap.putIfAbsent(indexableValue, ConcurrentHashMap.newKeySet());
            objects = keyMap.get(indexableValue);
        }
        objects.add(element);
    }

    private List<T> getNotModifiedElements(final String key, final Object value) {
        final Map<Object, Set<TinkerElementContainer<T>>> keyMap = index.get(key);
        if (null == keyMap)
            return new ArrayList<>();

        Set<TinkerElementContainer<T>> set = keyMap.get(indexable(value));
        if (null == set)
            return new ArrayList<>();

        return set.stream().
                filter(e -> !e.isChanged() && e.get() != null).
                map(e -> e.get()).collect(Collectors.toList());
    }

    private Set<T> getModifiedElements(final String key, final Object value) {
        final Map<String, Map<Object, Set<T>>> index = txIndex.get();
        if (null == index)
            return null;

        final Map<Object, Set<T>> keyMap = index.get(key);
        if (null == keyMap)
            return null;

        return keyMap.get(indexable(value));
    }

    @Override
    public List<T> get(final String key, final Object value) {
        final List<T> notModifiedElements = getNotModifiedElements(key, value);
        final Set<T> modifiedElements = getModifiedElements(key, value);
        if (modifiedElements != null)
            modifiedElements.forEach(e -> {
                if (!notModifiedElements.contains(e)) notModifiedElements.add(e);
            });

        return notModifiedElements;
    }

    @Override
    public long count(final String key, final Object value) {
        return get(key, value).size();
    }

    @Override
    public void remove(final String key, final Object value, final T element) {
        final Map<String, Map<Object, Set<T>>> index = txIndex.get();
        if (null == index) return;

        final Map<Object, Set<T>> keyMap = index.get(key);
        if (null != keyMap) {
            final Set<T> objects = keyMap.get(indexable(value));
            if (null != objects) {
                objects.remove(element);
                if (objects.isEmpty())
                    keyMap.remove(indexable(value));
            }
        }
    }

    @Override
    public void removeElement(final T element) {
        if (indexClass.isAssignableFrom(element.getClass())) {
            element.properties().forEachRemaining(p -> {
                        if (p.isPresent() && indexedKeys.contains(p.key())) {
                            remove(p.key(), p.value(), element);
                        }
                    }
            );
        }
    }

    private void put(final String key, final Object value, final TinkerElementContainer<T> container) {
        Map<Object, Set<TinkerElementContainer<T>>> keyMap = index.get(key);
        if (null == keyMap) {
            index.putIfAbsent(key, new ConcurrentHashMap<>());
            keyMap = index.get(key);
        }
        final Object indexableValue = indexable(value);
        Set<TinkerElementContainer<T>> objects = keyMap.get(indexableValue);
        if (null == objects) {
            keyMap.putIfAbsent(indexableValue, ConcurrentHashMap.newKeySet());
            objects = keyMap.get(indexableValue);
        }
        objects.add(container);
    }

    private void addContainer(final TinkerElementContainer<T> container) {
        final T element = container.get();
        if (!indexClass.isAssignableFrom(element.getClass()) || !element.properties().hasNext())
            return;

        element.properties().forEachRemaining(p -> {
                    if (p.isPresent() && indexedKeys.contains(p.key())) {
                        put(p.key(), p.value(), container);
                    }
                }
        );
    }

    @Override
    public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
        if (indexedKeys.contains(key)) {
            remove(key, oldValue, element);
            putTxElement(key, newValue, element);
        }
    }

    @Override
    public void createIndex(final String key, final Map<String,Object> configuration) {
        if (null == key)
            throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (key.isEmpty())
            throw new IllegalArgumentException("The key for the index cannot be an empty string");

        if (indexedKeys.contains(key))
            return;
        indexedKeys.add(key);

        final Map elements =
                Vertex.class.isAssignableFrom(indexClass) ?
                        ((TinkerTransactionGraph) graph).getVertices() :
                        ((TinkerTransactionGraph) graph).getEdges();

        for (Object element : elements.values()) {
            addContainer((TinkerElementContainer<T>) element);
        }
    }

    @Override
    public void dropIndex(final String key) {
        if (index.containsKey(key))
            index.remove(key).clear();

        final Map<String, Map<Object, Set<T>>> index = txIndex.get();
        if (index != null && index.containsKey(key))
            index.remove(key).clear();

        indexedKeys.remove(key);
    }

    private void removeContainer(TinkerElementContainer<T> container) {
        final T element = container.getUnmodified();
        if (element == null || !indexClass.isAssignableFrom(element.getClass()) || !element.properties().hasNext())
            return;

        element.properties().forEachRemaining(p -> {
            final Map<Object, Set<TinkerElementContainer<T>>> keyMap = index.get(p.key());
            final Object indexableValue = indexable(p.value());
            if (null != keyMap) {
                final Set<TinkerElementContainer<T>> objects = keyMap.get(indexableValue);
                if (null != objects) {
                    objects.remove(container);
                    if (objects.isEmpty())
                        keyMap.remove(indexableValue);
                }
            }
        });
    }

    public void commit(final Set<TinkerElementContainer<T>> updatedElements) {
        for (final TinkerElementContainer<T> element : updatedElements) {
            removeContainer(element);
            if (!element.isDeleted())
                // todo: compare and update only changed properties
                addContainer(element);
        }

        txIndex.remove();
    }

    public void rollback() {
        txIndex.remove();
    }
}
