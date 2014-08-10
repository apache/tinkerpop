package com.tinkerpop.gremlin.console.plugin

import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin
import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class PluggedIn {
    private final GremlinPlugin plugin
    private boolean activated = false

    private final Groovysh shell
    private final IO io

    public PluggedIn(final GremlinPlugin plugin, final Groovysh shell, final IO io, final boolean activated) {
        this.plugin = plugin
        this.activated = activated
        this.shell = shell
        this.io = io
    }

    GremlinPlugin getPlugin() {
        return plugin
    }

    boolean getActivated() {
        return activated
    }

    void activate() {
        plugin.pluginTo(new ConsolePluginAcceptor(shell, io))
        this.activated = true
    }

    void deactivate() {
        this.activated = false
    }
}
