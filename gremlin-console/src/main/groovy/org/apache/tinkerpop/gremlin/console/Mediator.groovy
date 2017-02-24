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
package org.apache.tinkerpop.gremlin.console

import org.apache.tinkerpop.gremlin.console.plugin.PluggedIn
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Mediator {
    public final Map<String, PluggedIn> availablePlugins = [:]
    public final List<RemoteAcceptor> remotes = []
    public int position
    public boolean localEvaluation = true

    private final Console console

    private static String LINE_SEP = System.getProperty("line.separator")

    public static final String IMPORT_SPACE = "import "
    public static final String IMPORT_STATIC_SPACE = "import static "
    public static final String IMPORT_WILDCARD = ".*"
    public static final boolean useV3d3 = System.getProperty("plugins", "v3d2") == "v3d3"

    public Mediator(final Console console) {
        this.console = console
    }

    public RemoteAcceptor currentRemote() { return remotes.get(position) }

    def addRemote(final RemoteAcceptor remote) {
        remotes.add(remote)
        position = remotes.size() - 1
        return remote
    }

    def removeCurrent() {
        final RemoteAcceptor toRemove = remotes.remove(position)
        position = 0
        return toRemove
    }

    def RemoteAcceptor nextRemote() {
        position++
        if (position >= remotes.size()) position = 0
        return currentRemote()
    }

    def RemoteAcceptor previousRemote() {
        position--
        if (position < 0) position = remotes.size() - 1
        return currentRemote()
    }

    def showShellEvaluationOutput(final boolean show) {
        console.showShellEvaluationOutput(show)
    }

    def writePluginState() {
        def file = new File(ConsoleFs.PLUGIN_CONFIG_FILE)

        // ensure that the directories exist to hold the file.
        file.mkdirs()

        if (file.exists())
            file.delete()

        new File(ConsoleFs.PLUGIN_CONFIG_FILE).withWriter { out ->
            activePlugins().each { k, v -> out << (k + LINE_SEP) }
        }
    }

    def activePlugins() {
        availablePlugins.findAll { it.value.activated }
    }

    static def readPluginState() {
        def file = new File(ConsoleFs.PLUGIN_CONFIG_FILE)
        return file.exists() ? file.readLines() : []
    }

    def void close() {
        remotes.each { remote ->
            try {
                remote.close()
            } catch (Exception ignored) {

            }
        }
    }

}
