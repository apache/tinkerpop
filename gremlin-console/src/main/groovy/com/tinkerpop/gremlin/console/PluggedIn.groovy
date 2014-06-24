package com.tinkerpop.gremlin.console

import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class PluggedIn {
    private final GremlinPlugin plugin
    private boolean installed = false

    public PluggedIn(final GremlinPlugin plugin, final boolean installed) {
        this.plugin = plugin
        this.installed = installed
    }

    GremlinPlugin getPlugin() {
        return plugin
    }

    boolean getInstalled() {
        return installed
    }
}
