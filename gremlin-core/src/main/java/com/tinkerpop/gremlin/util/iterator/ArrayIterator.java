package com.tinkerpop.gremlin.util.iterator;

import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ArrayIterator<T> implements Iterator<T> {

    private final T[] array;
    private int current = 0;

    public ArrayIterator(final T[] array) {
        this.array = array;
    }

    public boolean hasNext() {
        return this.current < this.array.length;
    }

    public T next() {
        if (this.hasNext()) {
            this.current++;
            return this.array[this.current - 1];
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
