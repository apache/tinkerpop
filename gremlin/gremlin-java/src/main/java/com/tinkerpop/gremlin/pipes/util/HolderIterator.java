package com.tinkerpop.gremlin.pipes.util;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderIterator<T> implements Iterator<Holder<T>> {

    private final Holder head;
    private final Iterator<T> iterator;

    public HolderIterator(final Iterator<T> iterator) {
        this.iterator = iterator;
        this.head = null;
    }

    public HolderIterator(final Holder head, final Iterator<T> iterator) {
        this.iterator = iterator;
        this.head = head;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Holder<T> next() {
        return null == head ? new Holder<>(this.iterator.next()) : new Holder<>(this.iterator.next(), this.head);
    }
}
