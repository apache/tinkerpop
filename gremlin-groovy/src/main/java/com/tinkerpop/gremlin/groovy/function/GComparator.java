package com.tinkerpop.gremlin.groovy.function;

import groovy.lang.Closure;

import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GComparator<A> implements Comparator<A> {

    private final Closure closure;

    public GComparator(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public int compare(A first, A second) {
        return (int) this.closure.call(first, second);
    }

    public static GComparator[] make(final Closure... closures) {
        final GComparator[] comparators = new GComparator[closures.length];
        for (int i = 0; i < closures.length; i++) {
            comparators[i] = new GComparator(closures[i]);
        }
        return comparators;
    }
}
