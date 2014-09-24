package com.tinkerpop.gremlin.groovy.engine.function;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSPredicate<A> extends GLambda implements java.util.function.Predicate<A>, java.io.Serializable {

    public GSPredicate(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    @Override
    public boolean test(final A a) {
        try {
            return (boolean) STATIC_ENGINE.eval(this.gremlinGroovyScript, makeBindings(a));
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
