package com.tinkerpop.gremlin.groovy;

import com.tinkerpop.gremlin.util.function.SFunction;
import groovy.lang.Closure;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyFunction<A, B> implements SFunction<A, B> {

    private final Closure closure;

    public GroovyFunction(final Closure closure) {
        this.closure = closure;
    }

    public B apply(final A argument) {
        return (B) this.closure.call(argument);
    }

    /*public static PipeFunction[] generate(final Closure... closures) {
        final PipeFunction[] pipeFunctions = new PipeFunction[closures.length];
        for (int i = 0; i < closures.length; i++) {
            pipeFunctions[i] = new GroovyPipeFunction(null, closures[i]);
        }
        return pipeFunctions;
    }*/
}
