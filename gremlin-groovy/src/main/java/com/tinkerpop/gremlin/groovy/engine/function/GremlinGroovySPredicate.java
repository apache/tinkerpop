package com.tinkerpop.gremlin.groovy.engine.function;

import com.tinkerpop.gremlin.util.function.SPredicate;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinGroovySPredicate<A> extends GremlinGroovyLambda implements SPredicate<A> {

    public GremlinGroovySPredicate(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    public boolean test(final A a) {
        try {
            return (boolean) engine.eval(this.gremlinGroovyScript, makeBindings(a));
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }


}
