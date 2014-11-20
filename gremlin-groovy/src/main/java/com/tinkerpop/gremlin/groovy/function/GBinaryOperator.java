package com.tinkerpop.gremlin.groovy.function;

import groovy.lang.Closure;

import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GBinaryOperator<A> implements BinaryOperator<A> {

    private final Closure closure;

    public GBinaryOperator(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public A apply(A a, A b) {
        return (A) this.closure.call(a, b);
    }

    public static GBinaryOperator[] make(final Closure... closures) {
        final GBinaryOperator[] functions = new GBinaryOperator[closures.length];
        for (int i = 0; i < closures.length; i++) {
            functions[i] = new GBinaryOperator(closures[i]);
        }
        return functions;
    }
}
