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

import groovy.grape.Grape
import org.apache.groovy.groovysh.jline.GroovyEngine
import org.apache.tinkerpop.gremlin.console.ConsoleFs
import org.apache.tinkerpop.gremlin.console.Mediator
import org.apache.tinkerpop.gremlin.console.PluggedIn
import org.apache.tinkerpop.gremlin.groovy.util.Artifact
import org.apache.tinkerpop.gremlin.groovy.util.DependencyGrabber
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin

/**
 * Install a dependency into the console.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class InstallCommand extends GremlinCommand {

    private final Mediator mediator
    private final GroovyEngine engine

    public InstallCommand(final Mediator mediator, final GroovyEngine engine) {
        super("/install")
        this.mediator = mediator
        this.engine = engine
    }

    @Override
    String description() { "Install a Maven library and its dependencies into the Gremlin Console" }

    @Override
    String usage() { "<group> <module> <version>" }

    @Override
    String help() {
        "Import a Maven library into Gremlin.  In some cases, a dependency can only be included properly after " +
                "restart of the Console (a message will appear with such a notification if such action is necessary)."
    }

    @Override
    def Object execute(final List<String> arguments) {
        final def artifact = createArtifact(arguments)
        try {
            def grabber = new DependencyGrabber(engine.getClassLoader(), ConsoleFs.CONSOLE_HOME_DIR)
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
        ServiceLoader.load(GremlinPlugin, engine.getClassLoader()).forEach { plugin ->
            if (!mediator.availablePlugins.containsKey(plugin.class.name)) {
                mediator.availablePlugins.put(plugin.class.name, new PluggedIn((GremlinPlugin) plugin, engine, false))
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
