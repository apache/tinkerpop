package com.tinkerpop.gremlin.console.groovy.plugin;

import com.tinkerpop.gremlin.console.plugin.GephiRemoteAcceptor;
import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import com.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;

import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GephiGremlinPlugin extends AbstractGremlinPlugin {

    public GephiGremlinPlugin() {
        super(true);
    }

    @Override
    public String getName() {
        return "tinkerpop.gephi";
    }

    @Override
    public Optional<RemoteAcceptor> remoteAcceptor() {
        return Optional.of(new GephiRemoteAcceptor(shell, io));
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
        // do nothing
    }
}
