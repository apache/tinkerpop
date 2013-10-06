package com.tinkerpop.gremlin.pipes.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExpandableIterator<T> implements Iterator<T> {

    private final Queue<Iterator<T>> queue;

    @SafeVarargs
    public ExpandableIterator(final Iterator<T>... iterators) {
        this.queue = new LinkedList<>();
        for (final Iterator<T> iterator : iterators) {
            this.queue.add(iterator);
        }
    }

    public boolean hasNext() {
        for (Iterator<T> itty : this.queue) {
            if (itty.hasNext())
                return true;
        }
        return false;
    }

    public T next() {
        while (true) {
            final Iterator<T> itty = this.queue.element();
            if (null != itty && itty.hasNext()) return itty.next();
            else this.queue.remove();
        }
    }

    public void add(final Iterator<T> iterator) {
        this.queue.add(iterator);
    }
}
