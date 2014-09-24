package com.tinkerpop.gremlin.groovy.engine.function;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSBiConsumer<A, B> extends GLambda implements java.util.function.BiConsumer<A, B>, java.io.Serializable {

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
