package com.tinkerpop.gremlin.groovy;

import com.tinkerpop.gremlin.util.function.SPredicate;
import groovy.lang.Closure;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroovyPredicate<A> implements SPredicate<A> {

    private final Closure closure;

    public GroovyPredicate(final Closure closure) {
        this.closure = closure;
    }

    public boolean test(final A argument) {
        return (boolean) this.closure.call(argument);
    }
}
