package com.tinkerpop.gremlin.pipes;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExpandableIterator<T> implements Iterator<T> {

    private final Iterator<T> iterator;
    private final Queue<T> queue = new LinkedList<>();

    public ExpandableIterator(final Iterator<T> iterator) {
        this.iterator = iterator;
    }

    public boolean hasNext() {
        return !this.queue.isEmpty() || this.iterator.hasNext();
    }

    public T next() {
        if (!this.queue.isEmpty())
            return this.queue.remove();
        else
            return this.iterator.next();
    }

    public void add(final T t) {
        this.queue.add(t);
    }
}
