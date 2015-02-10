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
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ScriptEnginePluginAcceptor implements PluginAcceptor {
    private final ScriptEngine scriptEngine;

    public ScriptEnginePluginAcceptor(final ScriptEngine scriptEngine) {
        this.scriptEngine = scriptEngine;
    }

    @Override
    public void addBinding(final String key, final Object val) {
        scriptEngine.getContext().setAttribute(key, val, ScriptContext.GLOBAL_SCOPE);
    }

    @Override
    public Map<String, Object> getBindings() {
        return scriptEngine.getBindings(ScriptContext.GLOBAL_SCOPE);
    }

    /**
     * If the ScriptEngine implements the DependencyManager interface it will try to import the specified
     * import statements.
     */
    @Override
    public void addImports(final Set<String> importStatements) {
        if (this.scriptEngine instanceof DependencyManager)
            ((DependencyManager) this.scriptEngine).addImports(importStatements);
    }

    /**
     * Evaluate a script in the ScriptEngine.  Typically eval() should be called after imports as ScriptEngine
     * resets may occur during import.
     */
    @Override
    public Object eval(final String script) throws ScriptException {
        return this.scriptEngine.eval(script);
    }

    @Override
    public Map<String, Object> environment() {
        final Map<String, Object> env = new HashMap<>();
        env.put(GremlinPlugin.ENVIRONMENT, "scriptEngine");
        return env;
    }
}
