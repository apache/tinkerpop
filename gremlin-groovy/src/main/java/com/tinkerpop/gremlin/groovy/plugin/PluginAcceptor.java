package com.tinkerpop.gremlin.groovy.plugin;

import javax.script.ScriptException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface PluginAcceptor {
    /**
     * If the {@code PluginAcceptor} implements the DependencyManager interface it will try to import the specified
     * import statements.
     */
    public void addImports(final Set<String> importStatements);

    /**
     * Add a variable binding for the plugin host.
     */
    public void addBinding(final String key, final Object val);

    /**
     * Gets the list of bindings from the plugin host.  These bindings will represent the "global" binding list.
     */
    public Map<String, Object> getBindings();

    /**
     * Evaluate a script in the {@code PluginAcceptor}.
     */
    public Object eval(final String script) throws ScriptException;

    /**
     * Returns a map of implementation specific variables that can be referenced by the plugin. Those writing
     * plugins should examine the details of the various {@code PluginAcceptor} implementations for the variables
     * that they pass, as they may provide important information useful to the plugin itself.
     */
    public default Map<String, Object> environment() {
        return Collections.emptyMap();
    }
}
