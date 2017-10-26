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
package org.apache.tinkerpop.gremlin.console.commands

import org.apache.tinkerpop.gremlin.console.ConsoleFs
import org.apache.tinkerpop.gremlin.console.Mediator
import org.codehaus.groovy.tools.shell.ComplexCommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * Activate and manage a plugin.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class PluginCommand extends ComplexCommandSupport {
    private final Mediator mediator

    public PluginCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":plugin", ":pin", ["use", "list", "unuse"], "use")
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
