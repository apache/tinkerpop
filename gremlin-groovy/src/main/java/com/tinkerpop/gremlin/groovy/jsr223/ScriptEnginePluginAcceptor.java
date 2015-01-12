package com.tinkerpop.gremlin.groovy.jsr223;

import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ScriptEnginePluginAcceptor implements PluginAcceptor {
    private final ScriptEngine scriptEngine;

    public ScriptEnginePluginAcceptor(final ScriptEngine scriptEngine) {
        this.scriptEngine = scriptEngine;
    }

    @Override
    public void addBinding(final String key, final Object val) {
        scriptEngine.getContext().setAttribute(key, val, ScriptContext.GLOBAL_SCOPE);
    }

    @Override
    public Map<String, Object> getBindings() {
        return scriptEngine.getBindings(ScriptContext.GLOBAL_SCOPE);
    }

    /**
     * If the ScriptEngine implements the DependencyManager interface it will try to import the specified
     * import statements.
     */
    @Override
    public void addImports(final Set<String> importStatements) {
        if (this.scriptEngine instanceof DependencyManager)
            ((DependencyManager) this.scriptEngine).addImports(importStatements);
    }

    /**
     * Evaluate a script in the ScriptEngine.  Typically eval() should be called after imports as ScriptEngine
     * resets may occur during import.
     */
    @Override
    public Object eval(final String script) throws ScriptException {
        return this.scriptEngine.eval(script);
    }

    @Override
    public Map<String, Object> environment() {
        final Map<String, Object> env = new HashMap<>();
        env.put(GremlinPlugin.ENVIRONMENT, "scriptEngine");
        return env;
    }
}
