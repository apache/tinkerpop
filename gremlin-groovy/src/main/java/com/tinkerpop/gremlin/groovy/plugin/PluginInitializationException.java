package com.tinkerpop.gremlin.groovy.plugin;

/**
 * An exception that occurs as a result of plugin initialization, typically triggered by a bad evaluation in the
 * {@code ScriptEngine}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PluginInitializationException extends GremlinPluginException {
    public PluginInitializationException(final String message) {
        super(message);
    }

    public PluginInitializationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public PluginInitializationException(final Throwable cause) {
        super(cause);
    }
}
