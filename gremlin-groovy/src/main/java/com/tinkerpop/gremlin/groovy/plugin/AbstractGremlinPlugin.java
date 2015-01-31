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
    public static final String ENV_CONSOLE_IO = "ConsolePluginAcceptor.io";
    public static final String ENV_CONSOLE_SHELL = "ConsolePluginAcceptor.shell";

    protected static final String IMPORT_SPACE = "import ";
    protected static final String IMPORT_STATIC_SPACE = "import static ";
    protected static final String DOT_STAR = ".*";

    protected IO io;
    protected Groovysh shell;
    protected final boolean requireConsoleEnvironment;

    public AbstractGremlinPlugin() {
        this(false);
    }

    public AbstractGremlinPlugin(final boolean requireConsoleEnvironment) {
        this.requireConsoleEnvironment = requireConsoleEnvironment;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Provides a base implementation for plugins by grabbing the console environment variables and assigning them
     * to the {@link #io} and {@link #shell} member variables.
     *
     * @throws IllegalEnvironmentException if {@link #requireConsoleEnvironment} is set to true and if either
     *                                     the {@link #io} and {@link #shell} member variables are null.
     */
    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
        final Map<String, Object> environment = pluginAcceptor.environment();
        io = (IO) environment.get(ENV_CONSOLE_IO);
        shell = (Groovysh) environment.get(ENV_CONSOLE_SHELL);

        if (requireConsoleEnvironment && (null == io || null == shell))
            throw new IllegalEnvironmentException(this, ENV_CONSOLE_SHELL, ENV_CONSOLE_IO);

        try {
            afterPluginTo(pluginAcceptor);
        } catch (PluginInitializationException pie) {
            throw pie;
        } catch (Exception ex) {
            throw new PluginInitializationException(ex);
        }
    }

    /**
     * Called after the {@link #pluginTo(PluginAcceptor)} method is executed which sets the {@link #io} and
     * {@link #shell} member variables.
     */
    public abstract void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException;
}
