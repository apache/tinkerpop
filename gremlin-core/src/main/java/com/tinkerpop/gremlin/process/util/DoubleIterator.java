package com.tinkerpop.gremlin.process.util;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DoubleIterator<T> implements Iterator<T> {

    private final T a;
    private final T b;
    private char current = 'a';

    public DoubleIterator(final T a, final T b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean hasNext() {
        return this.current != 'x';
    }

    @Override
    public T next() {
        if (this.current == 'x')
            throw FastNoSuchElementException.instance();
        else {
            if (this.current == 'a') {
                this.current = 'b';
                return this.a;
            } else {
                this.current = 'x';
                return this.b;
            }
        }
    }
}
