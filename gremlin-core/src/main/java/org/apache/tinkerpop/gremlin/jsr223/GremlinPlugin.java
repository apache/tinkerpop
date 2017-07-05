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
package org.apache.tinkerpop.gremlin.jsr223;

import java.util.Optional;

/**
 * A plugin interface that is used by the {@link GremlinScriptEngineManager} to configure special {@link Customizer}
 * instances that will alter the features of any {@link GremlinScriptEngine} created by the manager itself.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GremlinPlugin {
    /**
     * The name of the module.  This name should be unique (use a namespaced approach) as naming clashes will
     * prevent proper module operations. Modules developed by TinkerPop will be prefixed with "tinkerpop."
     * For example, TinkerPop's implementation of Giraph would be named "tinkerpop.giraph".  If Facebook were
     * to do their own implementation the implementation might be called "facebook.giraph".
     */
    public String getName();

    /**
     * Some modules may require a restart of the plugin host for the classloader to pick up the features.  This is
     * typically true of modules that rely on {@code Class.forName()} to dynamically instantiate classes from the
     * root classloader (e.g. JDBC drivers that instantiate via @{code DriverManager}).
     */
    public default boolean requireRestart() {
        return false;
    }

    /**
     * Gets the list of all {@link Customizer} implementations to assign to a new {@link GremlinScriptEngine}. This is
     * the same as doing {@code getCustomizers(null)}.
     */
    public default Optional<Customizer[]> getCustomizers(){
        return getCustomizers(null);
    }

    /**
     * Gets the list of {@link Customizer} implementations to assign to a new {@link GremlinScriptEngine}. The
     * implementation should filter the returned {@code Customizers} according to the supplied name of the
     * Gremlin-enabled {@code ScriptEngine}. By providing a filter, {@code GremlinModule} developers can have the
     * ability to target specific {@code ScriptEngines}.
     *
     * @param scriptEngineName The name of the {@code ScriptEngine} or null to get all the available {@code Customizers}
     */
    public Optional<Customizer[]> getCustomizers(final String scriptEngineName);
}
