package com.tinkerpop.gremlin.groovy.jsr223;

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
     * Take maven coordinates and load the classes into the classloader used by the ScriptEngine.
     */
    void use(final String group, final String artifact, final String version);

    /**
     * Perform class imports for the ScriptEngine.
     */
    void addImports(final Set<String> importStatements);
}
