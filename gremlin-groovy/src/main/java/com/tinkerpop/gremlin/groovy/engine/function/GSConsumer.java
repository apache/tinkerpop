package com.tinkerpop.gremlin.groovy.engine.function;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSConsumer<A> extends GLambda implements java.util.function.Consumer<A>, java.io.Serializable {

    public GSConsumer(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    @Override
    public void accept(final A a) {
        try {
            STATIC_ENGINE.eval(this.gremlinGroovyScript, makeBindings(a));
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
