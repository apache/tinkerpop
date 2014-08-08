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
public abstract class GremlinGroovyLambda implements Serializable {

    protected final String gremlinGroovyScript;
    protected final static GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
    private static final List<String> VARIABLE_NAME_ARRAY = Arrays.asList("a", "b", "c", "d", "e", "f", "g");

    public GremlinGroovyLambda(final String gremlinGroovyScript) {
        this.gremlinGroovyScript = gremlinGroovyScript;
    }

    public static Bindings makeBindings(final Object... objects) {
        final Bindings bindings = new SimpleBindings();
        for (int i = 0; i < objects.length; i++) {
            bindings.put(VARIABLE_NAME_ARRAY.get(i), objects[i]);
        }
        return bindings;
    }

}
