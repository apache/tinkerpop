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
package org.apache.tinkerpop.gremlin.groovy.plugin;

import org.apache.tinkerpop.gremlin.groovy.jsr223.DependencyManager;

import javax.script.ScriptException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A {@link GremlinPlugin} can be used in multiple environments (e.g. ScriptEngine implementation).  Any environment
 * wishing to allow plugins should implement the {@code PluginAcceptor}.  It acts as an adapter to those environments
 * and provides the abstractions required for a plugin to work regardless of the environmental implementations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.4, replaced by {@link org.apache.tinkerpop.gremlin.jsr223.console.PluginAcceptor}.
 */
@Deprecated
public interface PluginAcceptor {
    /**
     * If the {@code PluginAcceptor} implements the {@link DependencyManager} interface it will try to import the
     * specified import statements.
     */
    public void addImports(final Set<String> importStatements);

    /**
     * Add a variable binding for the plugin host.
     */
    public void addBinding(final String key, final Object val);

    /**
     * Gets the list of bindings from the plugin host.  These bindings will represent the "global" binding list.
     */
    public Map<String, Object> getBindings();

    /**
     * Evaluate a script in the {@code PluginAcceptor}.
     */
    public Object eval(final String script) throws ScriptException;

    /**
     * Returns a map of implementation specific variables that can be referenced by the plugin. Those writing
     * plugins should examine the details of the various {@code PluginAcceptor} implementations for the variables
     * that they pass, as they may provide important information useful to the plugin itself.
     */
    public default Map<String, Object> environment() {
        return Collections.emptyMap();
    }
}
