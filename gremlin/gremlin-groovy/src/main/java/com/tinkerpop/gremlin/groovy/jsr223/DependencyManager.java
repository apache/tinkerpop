package com.tinkerpop.gremlin.groovy.jsr223;

import java.util.Map;
import java.util.Set;

/**
 * Provides a way to dynamically consume dependencies into the ScriptEngine classloader.  With Groovy this is
 * somewhat easily accomplished with Grape, but other ScriptEngine implementations might have ways to do it too,
 * so this interface makes that possible to expose.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface DependencyManager {
    /**
     * Take maven coordinates and load the classes into the classloader used by the ScriptEngine.  Those ScriptEngines
     * that can should support script engine plugins by checking if there are any new ScriptEnginePlugin
     * implementations in the classloader.  The GremlinGroovyScriptEngine implementation uses ServiceLoader to figure
     * out if there are such classes and then calls the plugInTo() method on that ScriptEnginePlugin interface.
     */
    void use(final String group, final String artifact, final String version);

    /**
     * Perform class imports for the ScriptEngine.
     */
    void addImports(final Set<String> importStatements);

    /**
     * List the dependencies in the ScriptEngine classloader.
     */
    Map[] dependencies();

    /**
     * List the imports in the ScriptEngine;
     */
    Map<String, Set<String>> imports();

    /**
     * Reset the ScriptEngine.  Clear caches and kill the classloader.
     */
    void reset();
}
