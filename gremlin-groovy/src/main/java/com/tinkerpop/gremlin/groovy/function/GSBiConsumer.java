package com.tinkerpop.gremlin.groovy.function;

import javax.script.ScriptException;
import java.util.function.BiConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSBiConsumer<A, B> extends GSLambda implements BiConsumer<A, B> {

    public GSBiConsumer(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    @Override
    public void accept(final A a, B b) {
        try {
            STATIC_ENGINE.eval(this.gremlinGroovyScript, makeBindings(a, b));
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
