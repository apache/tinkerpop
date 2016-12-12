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
import org.apache.tinkerpop.gremlin.console.plugin.PluggedIn
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin
import groovy.grape.Grape
import org.apache.tinkerpop.gremlin.groovy.util.Artifact
import org.apache.tinkerpop.gremlin.groovy.util.DependencyGrabber
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * Install a dependency into the console.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class InstallCommand extends CommandSupport {

    private final Mediator mediator

    public InstallCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":install", ":+")
        this.mediator = mediator
    }

    @Override
    def Object execute(final List<String> arguments) {
        final def artifact = createArtifact(arguments)
        try {
            def grabber = new DependencyGrabber(shell.getInterp().getClassLoader(), ConsoleFs.CONSOLE_HOME_DIR)
            grabber.copyDependenciesToPath(artifact)

            final def dep = grabber.makeDepsMap(artifact)
            final def pluginsThatNeedRestart = grabDeps(dep)
            return "Loaded: " + arguments + (pluginsThatNeedRestart.size() == 0 ? "" : " - restart the console to use $pluginsThatNeedRestart")
        } catch (Exception ex) {
            return ex.message
        }
    }

    private def grabDeps(final Map<String, Object> map) {
        Grape.grab(map)

        def pluginsThatNeedRestart = [] as Set

        // note that the service loader utilized the classloader from the groovy shell as shell class are available
        // from within there given loading through Grape.
        def pluginClass = mediator.useV3d3 ? org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin : GremlinPlugin
        ServiceLoader.load(pluginClass, shell.getInterp().getClassLoader()).forEach { plugin ->
            if (!mediator.availablePlugins.containsKey(plugin.class.name)) {

                if (Mediator.useV3d3) {
                    mediator.availablePlugins.put(plugin.class.name, new PluggedIn(new PluggedIn.GremlinPluginAdapter((org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin) plugin, shell, io), shell, io, false))
                } else {
                    mediator.availablePlugins.put(plugin.class.name, new PluggedIn((GremlinPlugin) plugin, shell, io, false))
                }

                //mediator.availablePlugins.put(plugin.class.name, new PluggedIn(plugin, shell, io, false))
                if (plugin.requireRestart())
                    pluginsThatNeedRestart << plugin.name
            }
        }

        return pluginsThatNeedRestart
    }

    private static def createArtifact(final List<String> arguments) {
        final String group = arguments.size() >= 1 ? arguments.get(0) : null
        final String module = arguments.size() >= 2 ? arguments.get(1) : null
        final String version = arguments.size() >= 3 ? arguments.get(2) : null

        if (group == null || group.isEmpty())
            throw new IllegalArgumentException("Group cannot be null or empty")

        if (module == null || module.isEmpty())
            throw new IllegalArgumentException("Module cannot be null or empty")

        if (version == null || version.isEmpty())
            throw new IllegalArgumentException("Version cannot be null or empty")

        return new Artifact(group, module, version)
    }
}