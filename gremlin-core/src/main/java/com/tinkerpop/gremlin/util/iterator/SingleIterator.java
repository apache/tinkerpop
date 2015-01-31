package com.tinkerpop.gremlin.util.iterator;

import com.tinkerpop.gremlin.process.FastNoSuchElementException;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class SingleIterator<T> implements Iterator<T> {

    private final T t;
    private boolean alive = true;

    protected SingleIterator(final T t) {
        this.t = t;
    }

    @Override
    public boolean hasNext() {
        return this.alive;
    }

    @Override
    public T next() {
        if (!this.alive)
            throw FastNoSuchElementException.instance();
        else {
            this.alive = false;
            return t;
        }
    }
}
