package com.tinkerpop.gremlin.groovy.jsr223;

import groovy.lang.GroovyClassLoader;
import org.codehaus.groovy.control.CompilerConfiguration;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyClassLoader extends GroovyClassLoader {
	public GremlinGroovyClassLoader(final ClassLoader parent, final CompilerConfiguration conf) {
		super(parent, conf);
	}

	@Override
	protected void removeClassCacheEntry(final String name) {
		super.removeClassCacheEntry(name);
	}
}
