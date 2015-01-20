package com.tinkerpop.gremlin.groovy.plugin;

import java.util.Optional;

/**
 * Those wanting to extend Gremlin can implement this interface to provide mapper imports and extension
 * methods to the language itself.  Gremlin uses ServiceLoader to install plugins.  It is necessary for
 * projects to include a com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin file in META-INF/services of their
 * packaged project which includes the full class names of the implementations of this interface to install.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GremlinPlugin {
    public static final String ENVIRONMENT = "GremlinPlugin.env";

    /**
     * The name of the plugin.  This name should be unique (use a namespaced approach) as naming clashes will
     * prevent proper plugin operations. Plugins developed by TinkerPop will be prefixed with "tinkerpop."
     * For example, TinkerPop's implementation of Giraph would be named "tinkerpop.giraph".  If Facebook were
     * to do their own implementation the implementation might be called "facebook.giraph".
     */
    public String getName();

    /**
     * Implementers will typically execute imports of classes within their project that they want available in the
     * console or they may use meta programming to introduce new extensions to the Gremlin.
     *
     * @throws IllegalEnvironmentException   if there are missing environment properties required by the plugin as
     *                                       provided from {@link PluginAcceptor#environment()}.
     * @throws PluginInitializationException if there is a failure in the plugin iniitalization process
     */
    public void pluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException;

    /**
     * Some plugins may require a restart of the plugin host for the classloader to pick up the features.  This is
     * typically true of plugins that rely on {@code Class.forName()} to dynamically instantiate classes from the
     * root classloader (e.g. JDBC drivers that instantiate via @{code DriverManager}).
     */
    public default boolean requireRestart() {
        return false;
    }

    /**
     * Allows a plugin to utilize features of the {@code :remote} and {@code :submit} commands of the Gremlin Console.
     * This method does not need to be implemented if the plugin is not meant for the Console for some reason or
     * if it does not intend to take advantage of those commands.
     */
    public default Optional<RemoteAcceptor> remoteAcceptor() {
        return Optional.empty();
    }
}
