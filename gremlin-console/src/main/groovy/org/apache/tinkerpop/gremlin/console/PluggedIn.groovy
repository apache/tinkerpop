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

import org.apache.groovy.groovysh.Groovysh
import org.apache.tinkerpop.gremlin.jsr223.BindingsCustomizer
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer
import org.apache.tinkerpop.gremlin.jsr223.ScriptCustomizer
import org.apache.tinkerpop.gremlin.jsr223.console.GremlinShellEnvironment
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
        plugin.getCustomizers("gremlin-groovy").get().each {
            if (it instanceof ImportCustomizer) {
                if (shell instanceof GremlinGroovysh) {
                    org.codehaus.groovy.control.customizers.ImportCustomizer ic = new org.codehaus.groovy.control.customizers.ImportCustomizer()
                    ic.addStarImports(it.getClassPackages().collect() { it.getName() }.toArray(new String[0]))
                    ic.addStaticStars(it.getMethodClasses().collect() { it.getCanonicalName() }.toArray(new String[0]))
                    ic.addStaticStars(it.getEnumClasses().collect() { it.getCanonicalName() }.toArray(new String[0]))
                    ((GremlinGroovysh) shell).getCompilerConfiguration().addCompilationCustomizers(ic)
                } else {
                    it.getClassPackages().collect {Mediator.IMPORT_SPACE + it.getName() + Mediator.IMPORT_WILDCARD }.each { shell.execute(it) }
                    it.getMethodClasses().collect {Mediator.IMPORT_STATIC_SPACE + it.getCanonicalName() + Mediator.IMPORT_WILDCARD}.each {shell.execute(it)}
                    it.getEnumClasses().collect {Mediator.IMPORT_STATIC_SPACE + it.getCanonicalName() + Mediator.IMPORT_WILDCARD}.each {shell.execute(it)}
                }
            } else if (it instanceof ScriptCustomizer) {
                it.getScripts().collect { it.join(LINE_SEPARATOR) }.each { shell.execute(it) }
            } else if (it instanceof BindingsCustomizer) {
                it.bindings.entrySet().each { kv -> shell.getInterp().getContext().setProperty(kv.key, kv.value) }
            }
        }
        this.activated = true
    }

    void deactivate() {
        this.activated = false
    }

    public class GroovyGremlinShellEnvironment implements GremlinShellEnvironment {

        @Override
        def <T> T getVariable(final String variableName) {
            return (T) shell.interp.context.getVariable(variableName)
        }

        @Override
        def <T> void setVariable(final String variableName, final T variableValue) {
            shell.interp.context.setVariable(variableName, variableValue)
        }

        @Override
        void println(final String line) {
            io.println(line)
        }

        @Override
        void errPrintln(final String line) {
            if (!Preferences.warnings) {
                return;
            }
            io.err.println("[warn] " + line);
        }

        @Override
        def <T> T execute(final String line) {
            return (T) shell.execute(line)
        }
    }
}
