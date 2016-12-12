/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.groovy.plugin;

import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;

import java.util.Map;

/**
 * A base class for a plugin that provides access to the shell and io variables.  This is a good class to extend
 * from if the plugin needs to interact with the shell in some way, such as the case with those plugins that
 * want to utilize the {@link org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor} and may need access to those
 * shell and io objects.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.4, replaced by {@link org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin}.
 */
@Deprecated
public abstract class AbstractGremlinPlugin implements GremlinPlugin {
    public static final String ENV_CONSOLE_IO = "ConsolePluginAcceptor.io";
    public static final String ENV_CONSOLE_SHELL = "ConsolePluginAcceptor.shell";

    protected static final String IMPORT_SPACE = "import ";
    protected static final String IMPORT_STATIC_SPACE = "import static ";
    protected static final String DOT_STAR = ".*";

    protected IO io;
    protected Groovysh shell;
    protected final boolean requireConsoleEnvironment;

    /**
     * Creates a new instance that does not force the plugin to require the console.  This will create a plugin that
     * will work in Gremlin Console and Gremlin Server.
     */
    public AbstractGremlinPlugin() {
        this(false);
    }

    /**
     * Creates a new instance that allows the plugin to specify whether the console is required or not.  It is only
     * necessary to require the console if there are specific required calls to {@code IO} or to {@code Groovysh}
     * methods in the plugin (as those classes are Gremlin Console related and cannot be provided outside of that
     * environment).  For a plugin to work in the Gremlin Console and in Gremlin Server this value must be set
     * to {@code false}.
     */
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
