package com.tinkerpop.gremlin.groovy.engine.function;

import com.tinkerpop.gremlin.util.function.TriConsumer;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSTriConsumer<A, B, C> extends GLambda implements TriConsumer<A, B, C> {

    public GSTriConsumer(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    @Override
    public void accept(final A a, B b, C c) {
        try {
            STATIC_ENGINE.eval(this.gremlinGroovyScript, makeBindings(a, b, c));
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
