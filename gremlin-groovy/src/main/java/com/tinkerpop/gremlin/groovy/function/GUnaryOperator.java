package com.tinkerpop.gremlin.groovy.function;

import groovy.lang.Closure;

import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GUnaryOperator<A> implements UnaryOperator<A> {

    private final Closure closure;

    public GUnaryOperator(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public A apply(A a) {
        return (A) this.closure.call(a);
    }

    public static GUnaryOperator[] make(final Closure... closures) {
        final GUnaryOperator[] functions = new GUnaryOperator[closures.length];
        for (int i = 0; i < closures.length; i++) {
            functions[i] = new GUnaryOperator(closures[i]);
        }
        return functions;
    }
}
