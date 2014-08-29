package com.tinkerpop.gremlin.groovy.function;

import com.tinkerpop.gremlin.util.function.SPredicate;
import groovy.lang.Closure;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GPredicate<A> implements SPredicate<A> {

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
            // TODO: Should we do this?
            // closures[i].dehydrate();
            functions[i] = new GPredicate(closures[i]);
        }
        return functions;
    }
}
