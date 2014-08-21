package com.tinkerpop.gremlin.groovy.engine.function;

import com.tinkerpop.gremlin.util.function.SPredicate;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GSPredicate<A> extends GLambda implements SPredicate<A> {

    public GSPredicate(final String gremlinGroovyScript) {
        super(gremlinGroovyScript);
    }

    public GSPredicate(final String gremlinGroovyScript, final boolean useStaticScriptEngine) {
        super(gremlinGroovyScript, useStaticScriptEngine);
    }

    public boolean test(final A a) {
        try {
            return (boolean) this.getEngine().eval(this.gremlinGroovyScript, makeBindings(a));
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
