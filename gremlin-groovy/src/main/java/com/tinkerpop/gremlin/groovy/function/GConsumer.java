package com.tinkerpop.gremlin.groovy.function;

import groovy.lang.Closure;

import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GConsumer<A> implements Consumer<A> {

    private final Closure closure;

    public GConsumer(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public void accept(A a) {
        this.closure.call(a);
    }

    public static GConsumer[] make(final Closure... closures) {
        final GConsumer[] functions = new GConsumer[closures.length];
        for (int i = 0; i < closures.length; i++) {
            functions[i] = new GConsumer(closures[i]);
        }
        return functions;
    }
}
