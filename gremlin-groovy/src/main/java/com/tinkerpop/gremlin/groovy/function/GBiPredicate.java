package com.tinkerpop.gremlin.groovy.function;

import com.tinkerpop.gremlin.util.function.SBiPredicate;
import groovy.lang.Closure;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GBiPredicate<A, B> implements SBiPredicate<A, B> {

    private final Closure closure;

    public GBiPredicate(final Closure closure) {
        this.closure = closure;
    }

    public boolean test(A a, B b) {
        return (boolean) this.closure.call(a, b);
    }

    public static GBiPredicate[] make(final Closure... closures) {
        final GBiPredicate[] functions = new GBiPredicate[closures.length];
        for (int i = 0; i < closures.length; i++) {
            // TODO: Should we do this?
            // closures[i].dehydrate();
            functions[i] = new GBiPredicate(closures[i]);
        }
        return functions;
    }
}
