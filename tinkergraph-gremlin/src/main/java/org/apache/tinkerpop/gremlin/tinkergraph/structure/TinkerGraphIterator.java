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

import org.apache.tinkerpop.gremlin.util.iterator.StoreIteratorCounter;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Wrapper on top of Iterator representing a closable resource to the underlying storage.
 *
 * This class also serves as a reference on how the providers can maintain a counter of the
 * underlying storage resources in their implementation. This counter in coordination with
 * gremlin-test suite can be used to detect cases when the query processor does not gracefully
 * release the underlying resources.
 */
public class TinkerGraphIterator<E> implements Iterator<E>, AutoCloseable {
    /**
     * Original iterator which is wrapped by this class
     */
    private Iterator<E> orig;
    private E next;
    /**
     * Represents if the iterator has been fully consumed
     */
    private boolean finished;

    @Override
    public boolean hasNext() {
        if (next != null) return true;
        if (finished) return false;

        return tryComputeNext();
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final E ret = next;

        // pre-fetch the next value. It is important to do this
        // so that the underlying resource can be closed after reading the
        // last value of the iterator.
        tryComputeNext();

        return ret;
    }

    private boolean tryComputeNext() {
        try {
            next = orig.next();
            return true;
        } catch (NoSuchElementException ex) {
            close();
            next = null;
            return false;
        }
    }

    public TinkerGraphIterator(final Iterator<E> orig) {
        this.orig = orig;
        StoreIteratorCounter.INSTANCE.increment();
        finished = false;
    }

    @Override
    public void close() {
        if (!finished) {
            StoreIteratorCounter.INSTANCE.decrement();
        }
        finished = true;
    }
}
