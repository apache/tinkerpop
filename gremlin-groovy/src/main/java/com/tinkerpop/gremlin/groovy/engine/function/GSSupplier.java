package com.tinkerpop.gremlin.groovy.engine.function;

import com.tinkerpop.gremlin.util.function.SSupplier;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSSupplier<A> extends GLambda implements SSupplier<A> {

    public GSSupplier(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    @Override
    public A get() {
        try {
            return (A) STATIC_ENGINE.eval(this.gremlinGroovyScript);
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
