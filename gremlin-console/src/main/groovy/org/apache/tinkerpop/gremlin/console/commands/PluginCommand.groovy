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

    /**
     * Runs the {@code do_<function>} closure, as groovysh 4's {@code ComplexCommandSupport} did.
     */
    @Override
    Object execute(final List<String> arguments) {
        final String function = arguments.isEmpty() ? "use" : arguments[0]
        if (!(function in FUNCTIONS)) fail("Unknown function name: '$function'. Valid arguments: $FUNCTIONS")
        return ((Closure) this."do_$function").call(arguments.drop(1))
    }

    def Object do_use = { List<String> arguments ->
        final pluginName = arguments.size() == 1 ? arguments[0] : null
        if (pluginName == null || pluginName.isEmpty()) return "Specify the name of the plugin to use"

        if (!mediator.availablePlugins.values().any { it.plugin.name == pluginName })
            return "$pluginName could not be found - use ':plugin list' to see available plugins"

        mediator.showShellEvaluationOutput(false)
        mediator.availablePlugins.values().find { it.plugin.name == pluginName }.activate()
        mediator.showShellEvaluationOutput(true)
        if (mediator.writePluginState())
            return "$pluginName activated"
        else
            return "$pluginName activated but " + ConsoleFs.PLUGIN_CONFIG_FILE + " is readonly - the plugin list will remain unchanged on console restart"
    }

    def Object do_unuse = { List<String> arguments ->
        final pluginName = arguments.size() == 1 ? arguments[0] : null
        if (pluginName == null || pluginName.isEmpty()) return "Specify the name of the plugin to deactivate"

        if (!mediator.availablePlugins.values().any { it.plugin.name == pluginName })
            return "$pluginName could not be found - use ':plugin list' to see available plugins"

        mediator.availablePlugins.values().find { it.plugin.name == pluginName }.deactivate()
        if (mediator.writePluginState())
            return "$pluginName deactivated - restart your console for this to take effect"
        else
            return "$pluginName deactivated but " + ConsoleFs.PLUGIN_CONFIG_FILE + " is readonly - the plugin list will remain unchanged on console restart"
    }

    def do_list = { List<String> arguments ->
        return mediator.availablePlugins.collect { k, v -> v.plugin.name + (v.activated ? "[active]" : "") }
    }
}
