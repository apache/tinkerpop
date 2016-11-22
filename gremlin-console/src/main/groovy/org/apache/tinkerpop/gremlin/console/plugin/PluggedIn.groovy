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
package org.apache.tinkerpop.gremlin.console.plugin

import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteException
import org.apache.tinkerpop.gremlin.jsr223.BindingsCustomizer
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer
import org.apache.tinkerpop.gremlin.jsr223.ScriptCustomizer
import org.apache.tinkerpop.gremlin.jsr223.console.ConsoleCustomizer
import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class PluggedIn {
    private static final String LINE_SEPARATOR = System.getProperty("line.separator")
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

    public static class GremlinPluginAdapter implements GremlinPlugin {
        org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin corePlugin

        public GremlinPluginAdapter(final org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin corePlugin) {
            this.corePlugin = corePlugin
        }

        @Override
        String getName() {
            return corePlugin.getName()
        }

        @Override
        void pluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
            corePlugin.getCustomizers("gremlin-groovy").get().each {
                if (it instanceof ImportCustomizer) {
                    def imports = [] as Set
                    it.classImports.each { imports.add("import " + it.canonicalName )}
                    it.methodImports.each { imports.add("import static " + it.declaringClass.canonicalName + "." + it.name) }
                    it.enumImports.each { imports.add("import static " + it.declaringClass.canonicalName + "." + it.name()) }
                    pluginAcceptor.addImports(imports)
                } else if (it instanceof ScriptCustomizer) {
                    it.getScripts().collect { it.join(LINE_SEPARATOR) }.each { pluginAcceptor.eval(it) }
                } else if (it instanceof BindingsCustomizer) {
                    it.bindings.entrySet().each { kv -> pluginAcceptor.addBinding(kv.key, kv.value) }
                }
            }
        }

        @Override
        boolean requireRestart() {
            return corePlugin.requireRestart()
        }

        @Override
        Optional<RemoteAcceptor> remoteAcceptor() {
            // find a consoleCustomizer if available
            if (!corePlugin.getCustomizers("gremlin-groovy").any{ it instanceof ConsoleCustomizer })
                Optional.empty()

            ConsoleCustomizer customizer = (ConsoleCustomizer) corePlugin.getCustomizers("gremlin-groovy").find{ it instanceof ConsoleCustomizer }
            return Optional.of(new RemoteAcceptorAdapter(customizer.remoteAcceptor))
        }
    }

    public static class RemoteAcceptorAdapter implements RemoteAcceptor {

        private org.apache.tinkerpop.gremlin.jsr223.console.RemoteAcceptor remoteAcceptor

        public RemoteAcceptorAdapter(org.apache.tinkerpop.gremlin.jsr223.console.RemoteAcceptor remoteAcceptor) {
            this.remoteAcceptor = remoteAcceptor
        }

        @Override
        Object connect(final List<String> args) throws RemoteException {
            return remoteAcceptor.connect(args)
        }

        @Override
        Object configure(final List<String> args) throws RemoteException {
            return remoteAcceptor.configure(args)
        }

        @Override
        Object submit(final List<String> args) throws RemoteException {
            return remoteAcceptor.submit(args)
        }

        @Override
        void close() throws IOException {
            remoteAcceptor.close()
        }
    }
}
