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
package org.apache.tinkerpop.gremlin.console.jsr223;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Host;
import org.apache.tinkerpop.gremlin.driver.LoadBalancingStrategy;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteTraversal;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.console.ConsoleCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.console.GremlinShellEnvironment;
import org.apache.tinkerpop.gremlin.jsr223.console.RemoteAcceptor;
import org.apache.tinkerpop.gremlin.util.MessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.TokensV4;
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessageV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseResultV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerTokensV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.util.ser.SerializersV4;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "tinkerpop.server";

    private static final ImportCustomizer imports = DefaultImportCustomizer.build()
            .addClassImports(Cluster.class,
                    Client.class,
                    HttpResponseStatus.class,
                    Host.class,
                    LoadBalancingStrategy.class,
                    MessageSerializerV4.class,
                    Result.class,
                    ResultSet.class,
                    TokensV4.class,
                    ConnectionException.class,
                    ResponseException.class,
                    RequestMessageV4.class,
                    ResponseMessageV4.class,
                    ResponseResultV4.class,
                    ResponseStatusV4.class,
                    GraphSONMessageSerializerV4.class,
                    GraphSONUntypedMessageSerializerV4.class,
                    GraphBinaryMessageSerializerV4.class,
                    SerializationException.class,
                    SerializersV4.class,
                    SerTokensV4.class,
                    DriverRemoteConnection.class,
                    DriverRemoteTraversal.class).create();

    public DriverGremlinPlugin() {
        super(NAME, imports, new DriverConsoleCustomizer());
    }

    private static class DriverConsoleCustomizer implements ConsoleCustomizer {
        @Override
        public RemoteAcceptor getRemoteAcceptor(final GremlinShellEnvironment environment) {
            return new DriverRemoteAcceptor(environment);
        }
    }
}
