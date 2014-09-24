package com.tinkerpop.gremlin.groovy.function;

import javax.script.ScriptException;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSPredicate<A> extends GSLambda implements Predicate<A> {

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
