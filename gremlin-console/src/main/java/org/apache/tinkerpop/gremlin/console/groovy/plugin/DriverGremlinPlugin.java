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

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.SerTokens;
import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.4, replaced by {@link org.apache.tinkerpop.gremlin.console.jsr223.DriverGremlinPlugin}.
 */
@Deprecated
public class DriverGremlinPlugin extends AbstractGremlinPlugin {

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + Cluster.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + ConnectionException.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + RequestMessage.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + SerTokens.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_SPACE + DriverRemoteConnection.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "tinkerpop.server";
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public Optional<RemoteAcceptor> remoteAcceptor() {
        return null == this.shell ? Optional.empty() : Optional.of(new DriverRemoteAcceptor(this.shell));
    }
}
