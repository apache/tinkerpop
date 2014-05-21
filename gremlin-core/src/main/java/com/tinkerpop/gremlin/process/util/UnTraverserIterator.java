package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traverser;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnTraverserIterator<T> implements Iterator<T> {

    private final Iterator<Traverser<T>> iterator;

    public UnTraverserIterator(final Iterator<Traverser<T>> iterator) {
        this.iterator = iterator;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public T next() {
        return this.iterator.next().get();
    }
}
