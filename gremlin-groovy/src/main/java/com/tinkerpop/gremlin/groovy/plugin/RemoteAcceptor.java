package com.tinkerpop.gremlin.groovy.plugin;

import java.io.Closeable;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface RemoteAcceptor extends Closeable {
	public Object connect(final List<String> args);
	public Object configure(final List<String> args);
	public Object submit(final List<String> args);
}
