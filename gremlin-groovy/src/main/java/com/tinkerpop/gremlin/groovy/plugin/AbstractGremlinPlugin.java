package com.tinkerpop.gremlin.groovy.plugin;

import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;

import java.util.Map;

/**
 * A base class for a plugin that provides access to the shell and io variables.  This is a good class to extend
 * from if the plugin needs to interact with the shell in some way, such as the case with those plugins that
 * want to utilize the {@link com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor} and may need access to those
 * shell and io objects.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinPlugin implements GremlinPlugin {
    protected static final String IMPORT = "import ";
    protected static final String DOT_STAR = ".*";

    protected IO io;
    protected Groovysh shell;

    /**
     * {@inheritDoc}
     * <p>
     * This method may be overriden but take care to call this implementation so the shell and io variables get set.
     */
    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        final Map<String, Object> environment = pluginAcceptor.environment();
        this.io = (IO) environment.get("ConsolePluginAcceptor.io");
        this.shell = (Groovysh) environment.get("ConsolePluginAcceptor.shell");
    }
}
