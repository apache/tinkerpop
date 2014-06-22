package com.tinkerpop.gremlin.console;

import groovy.lang.Closure;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NullResultHookClosure extends Closure {
    public NullResultHookClosure(final Object owner) {
        super(owner);
    }

    public Object call(final Object[] args) {
        return null;
    }
}
