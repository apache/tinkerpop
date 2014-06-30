package com.tinkerpop.gremlin.console.plugin;

import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;

import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverGremlinPlugin extends AbstractGremlinPlugin {

	public DriverGremlinPlugin() {
	}

	@Override
	public String getName() {
		return "server";
	}

	@Override
	public Optional<RemoteAcceptor> remoteAcceptor() {
		return Optional.of(new DriverRemoteAcceptor(shell));
	}
}
