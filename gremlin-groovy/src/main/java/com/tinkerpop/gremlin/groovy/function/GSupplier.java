package com.tinkerpop.gremlin.groovy.function;

import groovy.lang.Closure;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSupplier<A> implements java.util.function.Supplier<A>, java.io.Serializable {

    private final Closure closure;

    public GSupplier(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public A get() {
        return (A) this.closure.call();
    }

    public static GSupplier[] make(final Closure... closures) {
        final GSupplier[] suppliers = new GSupplier[closures.length];
        for (int i = 0; i < closures.length; i++) {
            suppliers[i] = new GSupplier(closures[i]);
        }
        return suppliers;
    }
}
