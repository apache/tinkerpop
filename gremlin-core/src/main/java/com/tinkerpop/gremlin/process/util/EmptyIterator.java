package com.tinkerpop.gremlin.process.util;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class EmptyIterator<T> implements Iterator<T>, Serializable {

    public boolean hasNext() {
        return false;
    }

    public T next() {
        throw FastNoSuchElementException.instance();
    }
}
