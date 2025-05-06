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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Storage for indexes that can be used by different implementations of {@link AbstractTinkerGraph}.
 *
 * @param <T> type of {@link Element} to be indexed
 *
 * @author Valentyn Kahamlyk
 */
abstract class AbstractTinkerIndex<T extends Element> {

    protected final Class<T> indexClass;
    protected final AbstractTinkerGraph graph;
    protected final Set<String> indexedKeys = new HashSet<>();

    protected AbstractTinkerIndex(final AbstractTinkerGraph graph, final Class<T> indexClass) {
        this.graph = graph;
        this.indexClass = indexClass;
    }

    /**
     * Get list of elements which have a property with the desired value.
     * @param key property key
     * @param value property value
     * @return list of elements
     */
    public abstract List<T> get(final String key, final Object value);

    /**
     * Get count of elements which have a property with the desired value.
     * @param key property key
     * @param value property value
     * @return count of elements
     */
    public abstract long count(final String key, final Object value);

    /**
     * Remove elements with some property from index.
     * Convenient to use when removed only one property of an element.
     * @param key property key
     * @param value property value
     * @param element changed element
     */
    public abstract void remove(final String key, final Object value, final T element);

    /**
     * Remove elements from all indexes.
     * Can be used when element was deleted or has changed more than 1 property.
     * @param element changed element
     */
    public abstract void removeElement(final T element);

    /**
     * Update index for single property of element.
     * @param key property key
     * @param newValue updated value of property
     * @param oldValue optional value of property before update
     * @param element changed element
     */
    public abstract void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element);

    /**
     * Create new index with no configuration.
     * @param key property key
     */
    public void createIndex(final String key) {
        createIndex(key, Collections.emptyMap());
    }

    /**
     * Create new index with a specified configuration.
     * @param key property key
     */
    public abstract void createIndex(final String key, final Map<String, Object> configuration);

    /**
     * Drop index
     * @param key property key
     */
    public abstract void dropIndex(final String key);

    /**
     * Get all index keys for Graph
     * @return set of index keys
     */
    public Set<String> getIndexedKeys() {
        return indexedKeys;
    }

    /**
     * Provides a way for an index to have a {@code null} value as {@code ConcurrentHashMap} will not allow a
     * {@code null} key.
     */
    public static Object indexable(final Object obj) {
        return null == obj ? IndexedNull.instance() : obj;
    }

    public static final class IndexedNull {
        private static final IndexedNull inst = new IndexedNull();

        private IndexedNull() {}

        static IndexedNull instance() {
            return inst;
        }

        @Override
        public int hashCode() {
            return 751912123;
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof IndexedNull;
        }
    }
}
