package com.tinkerpop.gremlin.groovy.function;

import com.tinkerpop.gremlin.util.function.SFunction;
import groovy.lang.Closure;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GFunction<A, B> implements SFunction<A, B> {

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
            // TODO: Should we do this?
            // closures[i].dehydrate();
            functions[i] = new GFunction(closures[i]);
        }
        return functions;
    }
}
