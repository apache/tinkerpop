package com.tinkerpop.gremlin.groovy.engine.function;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import com.tinkerpop.gremlin.util.function.SSupplier;

import javax.script.ScriptException;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinGroovySSupplier<A> implements SSupplier<A> {

    private final String groovyScript;
    private final static GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();

    public GremlinGroovySSupplier(final String groovyScript) {
        this.groovyScript = groovyScript;
    }

    public A get() {
        try {
            return (A) this.engine.eval(this.groovyScript);
        } catch (final ScriptException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
