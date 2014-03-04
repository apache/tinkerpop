package com.tinkerpop.gremlin.groovy.plugin;

import javax.script.ScriptException;
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
     * Evaluate a script in the {@code PluginAcceptor}.
     */
    public Object eval(final String script) throws ScriptException;
}
