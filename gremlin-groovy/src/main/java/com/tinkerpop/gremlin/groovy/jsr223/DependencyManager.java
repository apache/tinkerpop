package com.tinkerpop.gremlin.groovy.jsr223;

import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.GremlinPluginException;

import java.util.List;
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
     * that can support script engine plugins should check if there are any new {@link GremlinPlugin}
     * implementations in the classloader.  The {@link com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine}
     * implementation uses ServiceLoader to figure out if there are such classes to return.
     * <br/>
     * It is up to the caller to execute the
     * {@link GremlinPlugin#pluginTo(com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor)} method.  The reason for
     * this has to do with conflicts that can occur with mapper imports that are added via the
     * {@link com.tinkerpop.gremlin.groovy.ImportCustomizerProvider} and scripts executed through the
     * {@link com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor}. Generally speaking, all calls to this "use" method
     * should be complete prior to calling
     * {@link GremlinPlugin#pluginTo(com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor)}.
     */
    List<GremlinPlugin> use(final String group, final String artifact, final String version);

    /**
     * Load a list of {@link GremlinPlugin} instances.  These plugins are typically returned from calls to
     * {@link #use(String, String, String)}.
     *
     * @throws GremlinPluginException if there is a problem loading the plugin itself.
     */
    void loadPlugins(final List<GremlinPlugin> plugins) throws GremlinPluginException;

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
