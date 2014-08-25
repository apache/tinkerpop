package com.tinkerpop.gremlin.groovy.engine.function;

import com.tinkerpop.gremlin.util.function.SConsumer;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSConsumer<A> extends GLambda implements SConsumer<A> {

    public GSConsumer(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    public void accept(final A a) {
        try {
            STATIC_ENGINE.eval(this.gremlinGroovyScript, makeBindings(a));
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
