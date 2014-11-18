package com.tinkerpop.gremlin.groovy.plugin;

/**
 * An exception thrown when the environment variables passed via {@link PluginAcceptor#environment()} do not meet
 * the needs of the {@link GremlinPlugin}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IllegalEnvironmentException extends GremlinPluginException {
    public IllegalEnvironmentException(final GremlinPlugin plugin, final String... expectedKeys) {
        super(String.format("The %s plugin may not be compatible with this environment - requires the follow ScriptEngine environment keys [%s]", plugin.getName(), String.join(",", expectedKeys)));
    }
}
