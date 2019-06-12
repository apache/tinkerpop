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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * BulkSet is a weighted set (i.e. a multi-set). Objects are added along with a bulk counter the denotes how many times the object was added to the set.
 * Given that count-based compression (vs. enumeration) can yield large sets, methods exist that are long-based (2^64).
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BulkSet<S> extends AbstractSet<S> implements Set<S>, Serializable {
    private final Map<S, Long> map = new LinkedHashMap<>();

    @Override
    public int size() {
        return (int) this.longSize();
    }

    public int uniqueSize() {
        return this.map.size();
    }

    public long longSize() {
        return this.map.values().stream().collect(Collectors.summingLong(Long::longValue));
    }

    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    @Override
    public boolean contains(final Object s) {
        return this.map.containsKey(s);
    }

    @Override
    public boolean add(final S s) {
        return this.add(s, 1l);
    }

    @Override
    public boolean addAll(final Collection<? extends S> collection) {
        if (collection instanceof BulkSet) {
            ((BulkSet<S>) collection).map.forEach(this::add);
        } else {
            collection.iterator().forEachRemaining(this::add);
        }
        return true;
    }

    public void forEach(final BiConsumer<S, Long> consumer) {
        this.map.forEach(consumer);
    }

    public Map<S, Long> asBulk() {
        return Collections.unmodifiableMap(map);
    }

    public boolean add(final S s, final long bulk) {
        final Long current = this.map.get(s);
        if (current != null) {
            this.map.put(s, current + bulk);
            return false;
        } else {
            this.map.put(s, bulk);
            return true;
        }
    }

    public long get(final S s) {
        final Long bulk = this.map.get(s);
        return null == bulk ? 0 : bulk;
    }

    @Override
    public boolean remove(final Object s) {
        return this.map.remove(s) != null;
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public Spliterator<S> spliterator() {
        return this.toList().spliterator();
    }

    @Override
    public boolean removeAll(final Collection<?> collection) {
        Objects.requireNonNull(collection);
        boolean modified = false;
        for (final Object object : collection) {
            if (null != this.map.remove(object))
                modified = true;
        }
        return modified;
    }

    @Override
    public int hashCode() {
        return this.map.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof BulkSet && this.map.equals(((BulkSet) object).map);
    }

    @Override
    public String toString() {
        return this.map.toString();
    }

    private List<S> toList() {
        final List<S> list = new ArrayList<>();
        this.map.forEach((k, v) -> {
            for (long i = 0; i < v; i++) {
                list.add(k);
            }
        });
        return list;
    }

    @Override
    public Iterator<S> iterator() {
        return new Iterator<S>() {
            final Iterator<Map.Entry<S, Long>> entryIterator = map.entrySet().iterator();
            S lastObject = null;
            long lastCount = 0l;

            public boolean hasNext() {
                return this.lastCount > 0l || this.entryIterator.hasNext();
            }

            @Override
            public S next() {
                if (this.lastCount > 0l) {
                    this.lastCount--;
                    return this.lastObject;
                }
                final Map.Entry<S, Long> entry = entryIterator.next();
                if (entry.getValue() == 1) {
                    return entry.getKey();
                } else {
                    this.lastObject = entry.getKey();
                    this.lastCount = entry.getValue() - 1;
                    return this.lastObject;
                }
            }
        };
    }
}
