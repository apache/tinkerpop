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
package org.apache.tinkerpop.gremlin.groovy.util;

import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;

import javax.script.ScriptException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TestableConsolePluginAcceptor implements PluginAcceptor {

    public static final String ENVIRONMENT_NAME = "console";
    public static final String ENVIRONMENT_SHELL = "ConsolePluginAcceptor.shell";
    public static final String ENVIRONMENT_IO = "ConsolePluginAcceptor.io";

    private Groovysh shell = new Groovysh(new IO(System.in, new OutputStream() {
        @Override
        public void write(int b) throws IOException {

        }
    }, System.err));

    @Override
    public void addImports(final Set<String> importStatements) {
        importStatements.forEach(this.shell::execute);
    }

    @Override
    public void addBinding(final String key, final Object val) {
        this.shell.getInterp().getContext().setVariable(key, val);
    }

    @Override
    public Map<String, Object> getBindings() {
        return Collections.unmodifiableMap(this.shell.getInterp().getContext().getVariables());
    }

    @Override
    public Object eval(final String script) throws ScriptException {
        return this.shell.execute(script);
    }

    @Override
    public Map<String, Object> environment() {
        final Map<String, Object> env = new HashMap<>();
        env.put(GremlinPlugin.ENVIRONMENT, ENVIRONMENT_NAME);
        env.put(ENVIRONMENT_IO, this.shell.getIo());
        env.put(ENVIRONMENT_SHELL, this.shell);
        return env;
    }

}
