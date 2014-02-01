package com.tinkerpop.gremlin.util;

import com.tinkerpop.gremlin.Holder;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnHolderIterator<T> implements Iterator<T> {

    private final Iterator<Holder<T>> iterator;

    public UnHolderIterator(final Iterator<Holder<T>> iterator) {
        this.iterator = iterator;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public T next() {
        return this.iterator.next().get();
    }
}
