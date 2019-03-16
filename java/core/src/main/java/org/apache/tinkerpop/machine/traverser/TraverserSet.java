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
package org.apache.tinkerpop.machine.traverser;


import org.apache.tinkerpop.util.FastNoSuchElementException;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserSet<C, S> extends AbstractSet<Traverser<C, S>> implements Set<Traverser<C, S>>, Queue<Traverser<C, S>>, Serializable {

    private final Map<Traverser<C, S>, Traverser<C, S>> map = Collections.synchronizedMap(new LinkedHashMap<>());

    public TraverserSet() {

    }

    public TraverserSet(final Traverser<C, S> traverser) {
        if (traverser != null)
            this.map.put(traverser, traverser);
    }

    @Override
    public Iterator<Traverser<C, S>> iterator() {
        return this.map.values().iterator();
    }

    public Traverser<C, S> get(final Traverser<C, S> traverser) {
        return this.map.get(traverser);
    }

    @Override
    public int size() {
        return this.map.size();
    }

    public long bulkSize() {
        long bulk = 0L;
        for (final Traverser<C, S> traverser : this.map.values()) {
            bulk = bulk + traverser.coefficient().count();
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
    public boolean add(final Traverser<C, S> traverser) {
        final Traverser<C, S> existing = this.map.get(traverser);
        if (null == existing) {
            this.map.put(traverser, traverser);
            return true;
        } else {
            existing.coefficient().sum(traverser.coefficient().value());
            return false;
        }
    }

    @Override
    public boolean offer(final Traverser<C, S> traverser) {
        return this.add(traverser);
    }

    @Override
    public Traverser<C, S> remove() {  // pop, exception if empty
        final Iterator<Traverser<C, S>> iterator = this.map.values().iterator();
        if (!iterator.hasNext())
            throw FastNoSuchElementException.instance();
        final Traverser<C, S> next = iterator.next();
        iterator.remove();
        return next;
    }

    @Override
    public Traverser<C, S> poll() {  // pop, null if empty
        return this.map.isEmpty() ? null : this.remove();
    }

    @Override
    public Traverser<C, S> element() { // peek, exception if empty
        return this.iterator().next();
    }

    @Override
    public Traverser<C, S> peek() { // peek, null if empty
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
    public Spliterator<Traverser<C, S>> spliterator() {
        return this.map.values().spliterator();
    }

    @Override
    public String toString() {
        return this.map.values().toString();
    }

    /*public void sort(final Comparator<CompleteTraverser<S>> comparator) {
        final List<CompleteTraverser<C, S>> list = new ArrayList<>(this.map.size());
        IteratorUtils.removeOnNext(this.map.values().iterator()).forEachRemaining(list::add);
        Collections.sort(list, comparator);
        this.map.reset();
        list.forEach(traverser -> this.map.put(traverser, traverser));
    }

    public void shuffle() {
        final List<CompleteTraverser<C, S>> list = new ArrayList<>(this.map.size());
        IteratorUtils.removeOnNext(this.map.values().iterator()).forEachRemaining(list::add);
        Collections.shuffle(list);
        this.map.reset();
        list.forEach(traverser -> this.map.put(traverser, traverser));
    }*/

}