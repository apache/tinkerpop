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
package org.apache.tinkerpop.gremlin.util.iterator;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * An {@code Iterator} that checks a {@code Predicate} prior to processing the specified child {@code Iterator}
 * instances.
 */
public final class AbortiveMultiIterator<T> implements Iterator<T>, Serializable, AutoCloseable {

    private final List<Iterator<T>> iterators = new ArrayList<>();
    private final List<Predicate<Long>> checks = new ArrayList<>();
    private long counter = 0;
    private int current = 0;

    /**
     * Adds an {@code Iterator} that will always be iterated.
     */
    public void addIterator(final Iterator<T> iterator) {
        addIterator(iterator, c -> true);
    }

    /**
     * Adds an {@code Iterator} that will iterate only if the {@code check} passes.
     *
     * @param check when returning {@code true} processing of the associated {@code Iterator} will proceed
     */
    public void addIterator(final Iterator<T> iterator, final Predicate<Long> check) {
        this.iterators.add(iterator);
        this.checks.add(check);
    }

    @Override
    public boolean hasNext() {
        if (this.current >= this.iterators.size())
            return false;

        Iterator<T> currentIterator = this.iterators.get(this.current);

        while (true) {
            if (currentIterator.hasNext()) {
                return true;
            } else {
                this.current++;
                if (this.current >= iterators.size() || !checks.get(current).test(counter))
                    break;
                currentIterator = iterators.get(this.current);
            }
        }
        return false;
    }

    @Override
    public void remove() {
        this.iterators.get(this.current).remove();
    }

    @Override
    public T next() {
        if (this.iterators.isEmpty()) throw FastNoSuchElementException.instance();

        Iterator<T> currentIterator = iterators.get(this.current);

        while (true) {
            if (currentIterator.hasNext()) {
                this.counter++;
                return currentIterator.next();
            } else {
                this.current++;
                if (this.current >= iterators.size() || !checks.get(current).test(counter))
                    break;
                currentIterator = iterators.get(current);
            }
        }
        throw FastNoSuchElementException.instance();
    }

    public void clear() {
        this.iterators.clear();
        this.checks.clear();
        this.counter = 0;
        this.current = 0;
    }

    /**
     * Close the underlying iterators if auto-closeable. Note that when Exception is thrown from any iterator
     * in the for loop on closing, remaining iterators possibly left unclosed.
     */
    @Override
    public void close() {
        for (Iterator<T> iterator : this.iterators) {
            CloseableIterator.closeIterator(iterator);
        }
    }
}
