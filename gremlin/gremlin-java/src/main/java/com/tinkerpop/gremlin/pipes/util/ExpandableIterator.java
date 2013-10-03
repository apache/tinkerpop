package com.tinkerpop.gremlin.pipes.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExpandableIterator<T> implements Iterator<T> {

    private final Queue<Iterator<T>> queue = new ConcurrentLinkedQueue<>();

    @SafeVarargs
    public ExpandableIterator(final Iterator<T>... iterators) {
        for (final Iterator<T> iterator : iterators) {
            this.queue.add(iterator);
        }
    }

    public boolean hasNext() {
        return this.queue.parallelStream().filter(i -> i.hasNext()).findAny().isPresent();
    }

    public T next() {
        if (this.queue.isEmpty())
            throw new NoSuchElementException();
        while (true) {
            final Iterator<T> itty = this.queue.element();
            if (itty.hasNext()) return itty.next();
            else this.queue.remove();
        }
    }

    public void add(final Iterator<T> iterator) {
        this.queue.add(iterator);
    }
}
