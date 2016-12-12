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

import org.apache.tinkerpop.gremlin.groovy.ImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPluginException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides a way to dynamically consume dependencies into the ScriptEngine classloader.  With Groovy this is
 * somewhat easily accomplished with Grape, but other ScriptEngine implementations might have ways to do it too,
 * so this interface makes that possible to expose.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.4, not replaced - no longer needed under the new {@link GremlinScriptEngine} model.
 */
@Deprecated
public interface DependencyManager {
    /**
     * Take maven coordinates and load the classes into the classloader used by the ScriptEngine.  Those ScriptEngines
     * that can support script engine plugins should check if there are any new {@link GremlinPlugin}
     * implementations in the classloader.  The {@link GremlinGroovyScriptEngine}
     * implementation uses ServiceLoader to figure out if there are such classes to return.
     * <p/>
     * It is up to the caller to execute the
     * {@link GremlinPlugin#pluginTo(org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor)} method.  The reason for
     * this has to do with conflicts that can occur with mapper imports that are added via the
     * {@link ImportCustomizerProvider} and scripts executed through the
     * {@link PluginAcceptor}. Generally speaking, all calls to this "use" method
     * should be complete prior to calling
     * {@link GremlinPlugin#pluginTo(PluginAcceptor)}.
     */
    List<GremlinPlugin> use(final String group, final String artifact, final String version);

    /**
     * Load a list of {@link GremlinPlugin} instances.  These plugins are typically returned from calls to
     * {@link #use(String, String, String)}.
     *
     * @throws GremlinPluginException if there is a problem loading the plugin itself.
     */
    void loadPlugins(final List<GremlinPlugin> plugins) throws GremlinPluginException;

    /**
     * Perform class imports for the ScriptEngine.
     */
    void addImports(final Set<String> importStatements);

    /**
     * List the dependencies in the ScriptEngine classloader.
     */
    Map[] dependencies();

    /**
     * List the imports in the ScriptEngine;
     */
    Map<String, Set<String>> imports();

    /**
     * Reset the ScriptEngine.  Clear caches and kill the classloader.
     */
    void reset();
}
