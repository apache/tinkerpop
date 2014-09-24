package com.tinkerpop.gremlin.groovy.function;

import javax.script.ScriptException;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSSupplier<A> extends GSLambda implements Supplier<A> {

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
