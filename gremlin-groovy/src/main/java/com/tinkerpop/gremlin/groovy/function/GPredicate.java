package com.tinkerpop.gremlin.groovy.function;

import groovy.lang.Closure;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GPredicate<A> implements Predicate<A> {

    private final Closure closure;

    public GPredicate(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public boolean test(A a) {
        return (boolean) this.closure.call(a);
    }

    public static GPredicate[] make(final Closure... closures) {
        final GPredicate[] functions = new GPredicate[closures.length];
        for (int i = 0; i < closures.length; i++) {
            functions[i] = new GPredicate(closures[i]);
        }
        return functions;
    }
}
