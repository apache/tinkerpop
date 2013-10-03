package com.tinkerpop.gremlin.pipes.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SingleIterator<T> implements Iterator<T> {

    private final T t;
    private boolean alive = true;

    public SingleIterator(final T t) {
        this.t = t;
    }

    public boolean hasNext() {
        return this.alive;
    }

    public T next() {
        if (!this.alive)
            throw new NoSuchElementException();
        else {
            this.alive = false;
            return t;
        }
    }
}
