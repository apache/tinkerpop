package com.tinkerpop.gremlin.process.util;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SingleIterator<T> implements Iterator<T> {

    private final T t;
    private boolean alive = true;

    public SingleIterator(final T t) {
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
