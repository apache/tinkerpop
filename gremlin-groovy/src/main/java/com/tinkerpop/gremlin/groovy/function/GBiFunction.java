package com.tinkerpop.gremlin.groovy.function;

import com.tinkerpop.gremlin.util.function.SBiFunction;
import groovy.lang.Closure;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GBiFunction<A, B, C> implements SBiFunction<A, B, C> {

    private final Closure closure;

    public GBiFunction(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public C apply(A a, B b) {
        return (C) this.closure.call(a, b);
    }

    public static GBiFunction[] make(final Closure... closures) {
        final GBiFunction[] functions = new GBiFunction[closures.length];
        for (int i = 0; i < closures.length; i++) {
            // TODO: Should we do this?
            // closures[i].dehydrate();
            functions[i] = new GBiFunction(closures[i]);
        }
        return functions;
    }
}
