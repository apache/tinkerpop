package com.tinkerpop.gremlin.console.util

import com.tinkerpop.gremlin.process.util.FastNoSuchElementException

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ArrayIterator implements Iterator {

    private final Object[] array
    private int count = 0

    public ArrayIterator(final Object[] array) {
        this.array = array
    }

    public void remove() {
        throw new UnsupportedOperationException()
    }

    public Object next() {
        if (count > array.length)
            throw FastNoSuchElementException.instance()

        return array[count++]
    }

    public boolean hasNext() {
        return count < array.length
    }
}
