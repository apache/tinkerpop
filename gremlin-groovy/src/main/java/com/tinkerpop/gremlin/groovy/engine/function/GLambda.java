package com.tinkerpop.gremlin.groovy.engine.function;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GLambda implements Serializable {

    protected final String gremlinGroovyScript;
    private final static GremlinGroovyScriptEngine STATIC_ENGINE = new GremlinGroovyScriptEngine();
    private static final List<String> VARIABLE_NAME_ARRAY = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
    private final boolean useStaticScriptEngine;

    public GLambda(final String gremlinGroovyScript) {
        this.gremlinGroovyScript = gremlinGroovyScript;
        this.useStaticScriptEngine = true;
    }

    public GLambda(final String gremlinGroovyScript, final boolean useStaticScriptEngine) {
        this.gremlinGroovyScript = gremlinGroovyScript;
        this.useStaticScriptEngine = useStaticScriptEngine;
    }

    public static Bindings makeBindings(final Object... objects) {
        final Bindings bindings = new SimpleBindings();
        for (int i = 0; i < objects.length; i++) {
            bindings.put(VARIABLE_NAME_ARRAY.get(i), objects[i]);
        }
        return bindings;
    }

    public GremlinGroovyScriptEngine getEngine() {
        return this.useStaticScriptEngine ? STATIC_ENGINE : new GremlinGroovyScriptEngine();
    }
}
