package com.tinkerpop.gremlin.groovy.engine.function;

import com.tinkerpop.gremlin.util.function.SFunction;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSFunction<A, B> extends GLambda implements SFunction<A, B> {

    public GSFunction(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    public GSFunction(final String gremlinGroovyScript, final boolean useStaticScriptEngine) {
        super(gremlinGroovyScript, useStaticScriptEngine);
    }

    public B apply(final A a) {
        try {
            return (B) this.getEngine().eval(this.gremlinGroovyScript, makeBindings(a));
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
