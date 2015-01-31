package com.tinkerpop.gremlin.console.commands

import com.tinkerpop.gremlin.console.Mediator
import org.codehaus.groovy.tools.shell.ComplexCommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * Activate a plugin.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class PluginCommand extends ComplexCommandSupport {
    private final Mediator mediator

    public PluginCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":plugin", ":pin", ["use", "list", "deactivate"], "use")
        this.mediator = mediator
    }

    def Object do_use = { List<String> arguments ->
        final pluginName = arguments.size() == 1 ? arguments[0] : null
        if (pluginName == null || pluginName.isEmpty()) return "Specify the name of the plugin to use"

        if (!mediator.availablePlugins.values().any { it.plugin.name == pluginName })
            return "$pluginName could not be found - use ':plugin list' to see available plugins"

        mediator.showShellEvaluationOutput(false)
        mediator.availablePlugins.values().find { it.plugin.name == pluginName }.activate()
        mediator.showShellEvaluationOutput(true)
        mediator.writePluginState()

        return "$pluginName activated"
    }

    def Object do_deactivate = { List<String> arguments ->
        final pluginName = arguments.size() == 1 ? arguments[0] : null
        if (pluginName == null || pluginName.isEmpty()) return "Specify the name of the plugin to deactivate"

        if (!mediator.availablePlugins.values().any { it.plugin.name == pluginName })
            return "$pluginName could not be found - use ':plugin list' to see available plugins"

        mediator.availablePlugins.values().find { it.plugin.name == pluginName }.deactivate()
        mediator.writePluginState()

        return "$pluginName deactivated - restart your console for this to take effect"
    }

    def do_list = { List<String> arguments ->
        return mediator.availablePlugins.collect { k, v -> v.plugin.name + (v.activated ? "[active]" : "") }
    }
}
