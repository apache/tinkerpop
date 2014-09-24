package com.tinkerpop.gremlin.groovy.engine.function;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSSupplier<A> extends GLambda implements java.util.function.Supplier<A>, java.io.Serializable {

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
