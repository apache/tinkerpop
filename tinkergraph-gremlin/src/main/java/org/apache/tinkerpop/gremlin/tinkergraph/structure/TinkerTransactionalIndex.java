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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TinkerTransactionalIndex<T extends TinkerElement> extends AbstractTinkerIndex<T> {

    protected Map<String, Map<Object, Set<TinkerElementContainer<T>>>> index = new ConcurrentHashMap<>();
    protected ThreadLocal<Map<String, Map<Object, Set<T>>>> txIndex =
            ThreadLocal.withInitial(() -> new ConcurrentHashMap<>());

    public TinkerTransactionalIndex(final TinkerTransactionGraph graph, final Class<T> indexClass) {
        super(graph, indexClass);
    }

    protected void putTxElement(final String key, final Object value, final T element) {
        final Map<String, Map<Object, Set<T>>> index = txIndex.get();

        Map<Object, Set<T>> keyMap = index.get(key);
        if (null == keyMap) {
            index.putIfAbsent(key, new ConcurrentHashMap<>());
            keyMap = index.get(key);
        }
        Set<T> objects = keyMap.get(indexable(value));
        if (null == objects) {
            keyMap.putIfAbsent(value, ConcurrentHashMap.newKeySet());
            objects = keyMap.get(value);
        }
        objects.add(element);
    }

    private List<T> getNotModifiedElements(final String key, final Object value) {
        final Map<Object, Set<TinkerElementContainer<T>>> keyMap = this.index.get(key);
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
    // remove from tx index
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
        if (this.indexClass.isAssignableFrom(element.getClass())) {
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
        Set<TinkerElementContainer<T>> objects = keyMap.get(value);
        if (null == objects) {
            keyMap.putIfAbsent(value, ConcurrentHashMap.newKeySet());
            objects = keyMap.get(value);
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
        if (this.indexedKeys.contains(key)) {
            this.remove(key, oldValue, element);
            this.putTxElement(key, newValue, element);
        }
    }

    @Override
    public void createKeyIndex(final String key) {
        if (null == key)
            throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (key.isEmpty())
            throw new IllegalArgumentException("The key for the index cannot be an empty string");

        if (this.indexedKeys.contains(key))
            return;
        this.indexedKeys.add(key);

        final Map elements =
                Vertex.class.isAssignableFrom(this.indexClass) ?
                        ((TinkerTransactionGraph) this.graph).vertices :
                        ((TinkerTransactionGraph) this.graph).edges;

        for (Object element : elements.values()) {
            addContainer((TinkerElementContainer<T>) element);
        }
    }

    @Override
    public void dropKeyIndex(final String key) {
        if (this.index.containsKey(key))
            this.index.remove(key).clear();

        final Map<String, Map<Object, Set<T>>> index = txIndex.get();
        if (index.containsKey(key))
            index.remove(key).clear();

        this.indexedKeys.remove(key);
    }

    @Override
    public Set<String> getIndexedKeys() {
        return this.indexedKeys;
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

    public void commit(final List<Map.Entry<Object, TinkerElementContainer<T>>> updatedElements) {
        for (final Map.Entry<Object, TinkerElementContainer<T>> pair : updatedElements) {
            removeContainer(pair.getValue());
            if (!pair.getValue().isDeleted())
                // todo: compare and update only changed properties
                addContainer(pair.getValue());
        }

        txIndex.set(null);
    }

    public void rollback() {
        txIndex.set(null);
    }
}
