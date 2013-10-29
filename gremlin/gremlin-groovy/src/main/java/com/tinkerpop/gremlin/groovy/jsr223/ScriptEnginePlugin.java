package com.tinkerpop.gremlin.groovy.jsr223;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Set;

/**
 * Those wanting to extend the Gremlin ScriptEngine can implement this interface to provide custom imports and extension
 * methods to the language itself.  Gremlin Console uses ServiceLoader to install plugins.  It is necessary for
 * projects wishing to extend the Console to include a com.tinkerpop.gremlin.groovy.jsr223.ConsolePlugin file in
 * META-INF/services of their packaged project which includes the full class names of the implementations of this
 * interface to install.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ScriptEnginePlugin {
    /**
     * The name of the plugin.  This name should be unique as naming clashes will prevent proper plugin operations.
     */
    String getName();

    /**
     * Implementors will typically execute imports of classes within their project that they want available in the
     * console or they may use meta programming to introduce new extensions to the Gremlin.
     */
    void pluginTo(final ScriptEngineController scriptEngineController);

    public class ScriptEngineController {
        private final ScriptEngine scriptEngine;

        public ScriptEngineController(final ScriptEngine scriptEngine) {
            this.scriptEngine = scriptEngine;
        }

        /**
         * If the ScriptEngine implements the DependencyManager interface it will try to import the specified
         * import statements.
         */
        public void addImports(final Set<String> importStatements) {
            if (this.scriptEngine instanceof DependencyManager)
                ((DependencyManager) this.scriptEngine).addImports(importStatements);
        }

        /**
         * Evaluate a script in the ScriptEngine.  Typically eval() should be called after imports as ScriptEngine
         * resets may occur during import.
         */
        public Object eval(final String script) throws ScriptException {
            return this.scriptEngine.eval(script);
        }
    }
}
