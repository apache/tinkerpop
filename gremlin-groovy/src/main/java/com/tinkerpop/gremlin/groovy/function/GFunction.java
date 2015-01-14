package com.tinkerpop.gremlin.groovy.function;

import groovy.lang.Closure;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GFunction<A, B> implements Function<A, B> {

    private final Closure closure;

    public GFunction(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public B apply(A a) {
        return (B) this.closure.call(a);
    }

    public static GFunction[] make(final Closure... closures) {
        final GFunction[] functions = new GFunction[closures.length];
        for (int i = 0; i < closures.length; i++) {
            functions[i] = new GFunction(closures[i]);
        }
        return functions;
    }

    @Override
    public String toString() {
        return "lambda";
    }
}
