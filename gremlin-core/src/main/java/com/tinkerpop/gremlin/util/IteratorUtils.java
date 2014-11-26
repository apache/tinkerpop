package com.tinkerpop.gremlin.util;

import java.util.Iterator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IteratorUtils {

    public static final <S, E> Iterator<E> map(final Iterator<S> iterator, final Function<S, E> function) {
        return new Iterator<E>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public E next() {
                return function.apply(iterator.next());
            }
        };
    }

    public static final <S, E> Iterable<E> map(final Iterable<S> iterable, final Function<S, E> function) {
        return new Iterable<E>() {
            @Override
            public Iterator<E> iterator() {
                return IteratorUtils.map(iterable.iterator(), function);
            }
        };
    }
}
