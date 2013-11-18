package com.tinkerpop.gremlin.server;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Features that ScriptEngines expose regardless of whether they are in a session or shared.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ScriptEngineOps {
    /**
     * Evaluate a script with Bindings for a particular language.
     */
    public Object eval(final String script, final Bindings bindings, final String language)
            throws ScriptException, InterruptedException, ExecutionException;

    /**
     * Perform append to the existing import list for all ScriptEngine instances that implement the DependencyManager
     * interface.
     */
    public void addImports(final Set<String> imports);

    /**
     * Pull in dependencies given some Maven coordinates.  Cycle through each ScriptEngine and determine if it
     * implements DependencyManager.  For those that do call the DependencyManager.use() method to fire it up.
     */
    public void use(final String group, final String artifact, final String version);

    /**
     * Get a list of all dependencies loaded to the ScriptEngine where the key is the ScriptEngine name and the value is the
     * dependency list for that ScriptEngine.
     */
    public Map<String,List<Map>> dependencies();

    /**
     * Gets a list of all imports in the ScriptEngine where the key is the ScriptEngine name and the value is the
     * import list for that ScriptEngine.
     */
    public Map<String, List<String>> imports();
}
