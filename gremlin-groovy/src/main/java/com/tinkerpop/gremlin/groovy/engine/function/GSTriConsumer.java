package com.tinkerpop.gremlin.groovy.engine.function;

import com.tinkerpop.gremlin.util.function.STriConsumer;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSTriConsumer<A, B, C> extends GLambda implements STriConsumer<A, B, C> {

    public GSTriConsumer(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    public void accept(final A a, B b, C c) {
        try {
            engine.eval(this.gremlinGroovyScript, makeBindings(a, b, c));
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
