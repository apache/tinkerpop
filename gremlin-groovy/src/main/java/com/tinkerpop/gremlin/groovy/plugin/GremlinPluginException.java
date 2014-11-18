package com.tinkerpop.gremlin.groovy.plugin;

/**
 * Base exception for {@link GremlinPlugin}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class GremlinPluginException extends Exception {
    public GremlinPluginException() {
    }

    public GremlinPluginException(final String message) {
        super(message);
    }

    public GremlinPluginException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public GremlinPluginException(final Throwable cause) {
        super(cause);
    }
}
