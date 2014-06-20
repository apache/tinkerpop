package com.tinkerpop.gremlin.giraph.groovy.plugin;

import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;

import java.io.IOException;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GiraphRemoteAcceptor implements RemoteAcceptor {
	@Override
	public Object connect(final List<String> args) {
		return null;
	}

	@Override
	public Object configure(final List<String> args) {
		return null;
	}

	@Override
	public Object submit(final List<String> args) {
		return null;
	}

	@Override
	public void close() throws IOException {

	}
}
