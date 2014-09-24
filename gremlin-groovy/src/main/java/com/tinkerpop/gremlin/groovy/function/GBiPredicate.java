package com.tinkerpop.gremlin.groovy.function;

import groovy.lang.Closure;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GBiPredicate<A, B> implements BiPredicate<A, B> {

    private final Closure closure;

    public GBiPredicate(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public boolean test(A a, B b) {
        return (boolean) this.closure.call(a, b);
    }

    public static GBiPredicate[] make(final Closure... closures) {
        final GBiPredicate[] functions = new GBiPredicate[closures.length];
        for (int i = 0; i < closures.length; i++) {
            functions[i] = new GBiPredicate(closures[i]);
        }
        return functions;
    }
}
