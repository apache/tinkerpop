package com.tinkerpop.gremlin.groovy.function;

import javax.script.ScriptException;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSConsumer<A> extends GSLambda implements Consumer<A> {

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
