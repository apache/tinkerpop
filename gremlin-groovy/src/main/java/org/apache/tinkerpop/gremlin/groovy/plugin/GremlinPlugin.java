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

import java.util.Optional;

/**
 * Those wanting to extend Gremlin can implement this interface to provide mapper imports and extension
 * methods to the language itself.  Gremlin uses {@code ServiceLoader} to install plugins.  It is necessary for
 * projects to include a {@code org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin} file in
 * {@code META-INF/services} of their packaged project which includes the full class names of the implementations
 * of this interface to install.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As for 3.2.4, replaced by {@link org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin}
 */
@Deprecated
public interface GremlinPlugin {
    public static final String ENVIRONMENT = "GremlinPlugin.env";

    /**
     * The name of the plugin.  This name should be unique (use a namespaced approach) as naming clashes will
     * prevent proper plugin operations. Plugins developed by TinkerPop will be prefixed with "tinkerpop."
     * For example, TinkerPop's implementation of Giraph would be named "tinkerpop.giraph".  If Facebook were
     * to do their own implementation the implementation might be called "facebook.giraph".
     */
    public String getName();

    /**
     * Implementers will typically execute imports of classes within their project that they want available in the
     * console or they may use meta programming to introduce new extensions to the Gremlin.
     *
     * @throws IllegalEnvironmentException   if there are missing environment properties required by the plugin as
     *                                       provided from {@link PluginAcceptor#environment()}.
     * @throws PluginInitializationException if there is a failure in the plugin iniitalization process
     */
    public void pluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException;

    /**
     * Some plugins may require a restart of the plugin host for the classloader to pick up the features.  This is
     * typically true of plugins that rely on {@code Class.forName()} to dynamically instantiate classes from the
     * root classloader (e.g. JDBC drivers that instantiate via @{code DriverManager}).
     */
    public default boolean requireRestart() {
        return false;
    }

    /**
     * Allows a plugin to utilize features of the {@code :remote} and {@code :submit} commands of the Gremlin Console.
     * This method does not need to be implemented if the plugin is not meant for the Console for some reason or
     * if it does not intend to take advantage of those commands.
     */
    public default Optional<RemoteAcceptor> remoteAcceptor() {
        return Optional.empty();
    }
}
