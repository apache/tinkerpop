package com.tinkerpop.gremlin.groovy.plugin;

/**
 * Those wanting to extend Gremlin can implement this interface to provide custom imports and extension
 * methods to the language itself.  Gremlin uses ServiceLoader to install plugins.  It is necessary for
 * projects to include a com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin file in META-INF/services of their
 * packaged project which includes the full class names of the implementations of this interface to install.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GremlinPlugin {
    /**
     * The name of the plugin.  This name should be unique as naming clashes will prevent proper plugin operations.
     */
    String getName();

    /**
     * Implementors will typically execute imports of classes within their project that they want available in the
     * console or they may use meta programming to introduce new extensions to the Gremlin.
     */
    void pluginTo(final PluginAcceptor pluginAcceptor);
}
