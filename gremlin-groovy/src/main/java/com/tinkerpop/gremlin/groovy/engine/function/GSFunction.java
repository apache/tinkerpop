package com.tinkerpop.gremlin.groovy.engine.function;

import javax.script.ScriptException;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSFunction<A, B> extends GLambda implements Function<A, B> {

    public GSFunction(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    @Override
    public B apply(final A a) {
        try {
            return (B) STATIC_ENGINE.eval(this.gremlinGroovyScript, makeBindings(a));
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
