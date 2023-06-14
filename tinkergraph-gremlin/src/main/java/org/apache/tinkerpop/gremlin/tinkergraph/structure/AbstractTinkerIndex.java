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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractTinkerIndex<T extends Element> {

    protected final Class<T> indexClass;
    protected final AbstractTinkerGraph graph;
    protected final Set<String> indexedKeys = new HashSet<>();

    protected AbstractTinkerIndex(final AbstractTinkerGraph graph, final Class<T> indexClass) {
        this.graph = graph;
        this.indexClass = indexClass;
    }

    public abstract List<T> get(final String key, final Object value);
    public abstract long count(final String key, final Object value);

    public abstract void remove(final String key, final Object value, final T element);

    public abstract void removeElement(final T element);

    public abstract void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element);

    public abstract void createKeyIndex(final String key);

    public abstract void dropKeyIndex(final String key);


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
