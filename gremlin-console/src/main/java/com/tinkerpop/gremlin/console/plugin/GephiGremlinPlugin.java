package com.tinkerpop.gremlin.console.plugin;

import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;

import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GephiGremlinPlugin extends AbstractGremlinPlugin {

    public GephiGremlinPlugin() {
    }

    @Override
    public String getName() {
        return "tinkerpop.gephi";
    }

    @Override
    public Optional<RemoteAcceptor> remoteAcceptor() {
        return Optional.of(new GephiRemoteAcceptor(shell, io));
    }
}
