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
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor
import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ConsolePluginAcceptor implements PluginAcceptor, org.apache.tinkerpop.gremlin.jsr223.console.PluginAcceptor {

    public static final String ENVIRONMENT_NAME = "console";
    public static final String ENVIRONMENT_SHELL = "ConsolePluginAcceptor.shell"
    public static final String ENVIRONMENT_IO = "ConsolePluginAcceptor.io"

    private final Groovysh shell
    private final IO io

    public ConsolePluginAcceptor(final Groovysh shell, final IO io) {
        this.shell = shell
        this.io = io
    }

    @Override
    void addBinding(final String key, final Object val) {
        shell.interp.context.setVariable(key, val)
    }

    @Override
    Map<String, Object> getBindings() {
        return Collections.unmodifiableMap(shell.interp.context.variables)
    }

    @Override
    void addImports(final Set<String> importStatements) {
        importStatements.each { this.shell.execute(it) }
    }

    @Override
    Object eval(final String script) throws javax.script.ScriptException { return this.shell.execute(script) }

    @Override
    Map<String, Object> environment() {
        return [(GremlinPlugin.ENVIRONMENT): ENVIRONMENT_NAME, (ENVIRONMENT_IO): io, (ENVIRONMENT_SHELL): shell]
    }
}