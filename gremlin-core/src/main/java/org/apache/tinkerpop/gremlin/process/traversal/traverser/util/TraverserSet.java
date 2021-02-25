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
package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserSet<S> extends AbstractSet<Traverser.Admin<S>> implements Set<Traverser.Admin<S>>, Queue<Traverser.Admin<S>>, Serializable {

    private final Map<Traverser.Admin<S>, Traverser.Admin<S>> map = Collections.synchronizedMap(new LinkedHashMap<>());

    public TraverserSet() {

    }

    public TraverserSet(final Traverser.Admin<S> traverser) {
        if (traverser != null)
            this.map.put(traverser, traverser);
    }

    @Override
    public Iterator<Traverser.Admin<S>> iterator() {
        return this.map.values().iterator();
    }

    public Traverser.Admin<S> get(final Traverser.Admin<S> traverser) {
        return this.map.get(traverser);
    }

    @Override
    public int size() {
        return this.map.size();
    }

    public long bulkSize() {
        long bulk = 0L;
        for (final Traverser.Admin<S> traverser : this.map.values()) {
            bulk = bulk + traverser.bulk();
        }
        return bulk;
    }

    @Override
    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    @Override
    public boolean contains(final Object traverser) {
        return this.map.containsKey(traverser);
    }

    @Override
    public boolean add(final Traverser.Admin<S> traverser) {
        final Traverser.Admin<S> existing = this.map.get(traverser);
        if (null == existing) {
            this.map.put(traverser, traverser);
            return true;
        } else {
            existing.merge(traverser);
            return false;
        }
    }

    @Override
    public boolean offer(final Traverser.Admin<S> traverser) {
        return this.add(traverser);
    }

    @Override
    public Traverser.Admin<S> remove() {  // pop, exception if empty
        final Iterator<Traverser.Admin<S>> iterator = this.map.values().iterator();
        if (!iterator.hasNext())
            throw FastNoSuchElementException.instance();
        final Traverser.Admin<S> next = iterator.next();
        iterator.remove();
        return next;
    }

    @Override
    public Traverser.Admin<S> poll() {  // pop, null if empty
        return this.map.isEmpty() ? null : this.remove();
    }

    @Override
    public Traverser.Admin<S> element() { // peek, exception if empty
        return this.iterator().next();
    }

    @Override
    public Traverser.Admin<S> peek() { // peek, null if empty
        return this.map.isEmpty() ? null : this.iterator().next();
    }

    @Override
    public boolean remove(final Object traverser) {
        return this.map.remove(traverser) != null;
    }

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public Spliterator<Traverser.Admin<S>> spliterator() {
        return this.map.values().spliterator();
    }

    @Override
    public String toString() {
        return this.map.values().toString();
    }

    public void sort(final Comparator<Traverser<S>> comparator) {
        final List<Traverser.Admin<S>> list = new ArrayList<>(this.map.size());
        IteratorUtils.removeOnNext(this.map.values().iterator()).forEachRemaining(list::add);
        Collections.sort(list, comparator);
        this.map.clear();
        list.forEach(traverser -> this.map.put(traverser, traverser));
    }

    public void shuffle(final Random random) {
        final List<Traverser.Admin<S>> list = new ArrayList<>(this.map.size());
        IteratorUtils.removeOnNext(this.map.values().iterator()).forEachRemaining(list::add);
        Collections.shuffle(list, random);
        this.map.clear();
        list.forEach(traverser -> this.map.put(traverser, traverser));
    }

}
