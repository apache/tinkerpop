/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.console.commands

import org.apache.tinkerpop.gremlin.console.ConsoleFs
import org.apache.tinkerpop.gremlin.console.Mediator
import org.apache.tinkerpop.gremlin.console.PluggedIn
import org.jline.reader.Completer
import org.jline.reader.impl.completer.StringsCompleter

/**
 * Activate and manage a plugin.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class PluginCommand extends GremlinCommand {
    private static final List<String> FUNCTIONS = ["use", "list", "unuse"]

    private final Mediator mediator

    public PluginCommand(final Mediator mediator) {
        super("/plugin", "/pin")
        this.mediator = mediator
    }

    @Override
    String description() { "Manage plugins for the Console" }

    @Override
    String usage() { "[use <plugin-name>|list|unuse <plugin-name>]" }

    @Override
    String help() { "Manage plugins for the Console." }

    @Override
    List<Completer> completers() {
        return [COMMAND_WORD,
                new StringsCompleter(FUNCTIONS as String[]),
                new StringsCompleter({ mediator.availablePlugins.values().collect { it.plugin.name } } as java.util.function.Supplier)]
    }

    @Override
    Object execute(final List<String> arguments) {
        final List<String> rest = arguments.drop(1)
        switch (arguments.isEmpty() ? "use" : arguments[0]) {
            case "use": return activate(rest)
            case "unuse": return deactivate(rest)
            case "list": return listPlugins()
            default: fail("Unknown function name: '${arguments[0]}'. Valid arguments: $FUNCTIONS")
        }
    }

    private Object activate(final List<String> arguments) {
        final String pluginName = arguments.size() == 1 ? arguments[0] : null
        if (pluginName == null || pluginName.isEmpty()) return "Specify the name of the plugin to use"
        final PluggedIn pluggedIn = findPlugin(pluginName)
        if (pluggedIn == null) return "$pluginName could not be found - use ':plugin list' to see available plugins"

        final boolean wasActivated = pluggedIn.activated
        pluggedIn.activate()
        if (mediator.writePluginState())
            return pluggedIn.loaded ? "$pluginName activated" : "$pluginName will be active after the console restarts"

        // the plugin list is unchanged, so neither is the state after a restart
        if (!wasActivated) pluggedIn.deactivate()
        if (!pluggedIn.loaded)
            return "$pluginName will not be active after the console restarts as " + ConsoleFs.PLUGIN_CONFIG_FILE + " is readonly"
        return "$pluginName activated but " + ConsoleFs.PLUGIN_CONFIG_FILE + " is readonly - the plugin list will remain unchanged on console restart"
    }

    private Object deactivate(final List<String> arguments) {
        final String pluginName = arguments.size() == 1 ? arguments[0] : null
        if (pluginName == null || pluginName.isEmpty()) return "Specify the name of the plugin to deactivate"
        final PluggedIn pluggedIn = findPlugin(pluginName)
        if (pluggedIn == null) return "$pluginName could not be found - use ':plugin list' to see available plugins"

        final boolean wasActivated = pluggedIn.activated
        pluggedIn.deactivate()
        if (mediator.writePluginState())
            return pluggedIn.loaded ? "$pluginName deactivated - restart your console for this to take effect" : "$pluginName deactivated"

        // an activated plugin is loaded or waits for a restart, so this only sets the flag back
        if (wasActivated) pluggedIn.activate()
        return "$pluginName deactivated but " + ConsoleFs.PLUGIN_CONFIG_FILE + " is readonly - the plugin list will remain unchanged on console restart"
    }

    private List<String> listPlugins() {
        return mediator.availablePlugins.values().collect { it.plugin.name + "[" + state(it) + "]" }
    }

    /**
     * Whether the plugin is active now, noting when that changes after a restart.
     */
    private static String state(final PluggedIn pluggedIn) {
        final String now = pluggedIn.loaded ? "active" : "inactive"
        return pluggedIn.loaded == pluggedIn.activated ? now : now + " until restart"
    }

    private PluggedIn findPlugin(final String pluginName) {
        return mediator.availablePlugins.values().find { it.plugin.name == pluginName }
    }
}
