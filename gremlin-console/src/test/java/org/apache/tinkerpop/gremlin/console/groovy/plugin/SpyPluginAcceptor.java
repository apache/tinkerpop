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
package org.apache.tinkerpop.gremlin.console.groovy.plugin;

import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

import javax.script.ScriptException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SpyPluginAcceptor implements PluginAcceptor {

    private final Set<String> imports = new HashSet<>();
    private final Map<String, Object> bindings = new HashMap<>();

    private final Function<String, Object> evalSpy;
    private final Supplier<Map<String, Object>> environment;

    public SpyPluginAcceptor() {
        this(script -> script);
    }

    public SpyPluginAcceptor(final Function<String, Object> evalSpy) {
        this(evalSpy, HashMap::new);
    }

    public SpyPluginAcceptor(final Supplier<Map<String, Object>> environmentMaker) {
        this(script -> script, environmentMaker);
    }

    public SpyPluginAcceptor(final Function<String, Object> evalSpy, final Supplier<Map<String, Object>> environmentMaker) {
        this.evalSpy = evalSpy;
        this.environment = environmentMaker;
    }

    @Override
    public void addImports(final Set<String> importStatements) {
        imports.addAll(importStatements);
    }

    @Override
    public void addBinding(final String key, final Object val) {
        bindings.put(key, val);
    }

    @Override
    public Map<String, Object> getBindings() {
        return bindings;
    }

    @Override
    public Object eval(final String script) throws ScriptException {
        return evalSpy.apply(script);
    }

    @Override
    public Map<String, Object> environment() {
        return environment.get();
    }

    public Set<String> getImports() {
        return imports;
    }
}
