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

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Host;
import org.apache.tinkerpop.gremlin.driver.LoadBalancingStrategy;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseResult;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatus;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteTraversal;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteTraversalSideEffects;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerGremlinV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerGremlinV2d0;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.driver.ser.GryoLiteMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.JsonBuilderGryoSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.SerTokens;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.console.ConsoleCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.console.GremlinShellEnvironment;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "tinkerpop.server";

    private static final ImportCustomizer imports = DefaultImportCustomizer.build()
            .addClassImports(Cluster.class,
                    Client.class,
                    Host.class,
                    LoadBalancingStrategy.class,
                    MessageSerializer.class,
                    Result.class,
                    ResultSet.class,
                    Tokens.class,
                    ConnectionException.class,
                    ResponseException.class,
                    RequestMessage.class,
                    ResponseMessage.class,
                    ResponseResult.class,
                    ResponseStatus.class,
                    ResponseStatusCode.class,
                    GraphSONMessageSerializerGremlinV1d0.class,
                    GraphSONMessageSerializerGremlinV2d0.class,
                    GraphSONMessageSerializerV1d0.class,
                    GraphSONMessageSerializerV2d0.class,
                    GryoLiteMessageSerializerV1d0.class,
                    GryoMessageSerializerV1d0.class,
                    JsonBuilderGryoSerializer.class,
                    MessageTextSerializer.class,
                    SerializationException.class,
                    Serializers.class,
                    SerTokens.class,
                    DriverRemoteConnection.class,
                    DriverRemoteTraversal.class,
                    DriverRemoteTraversalSideEffects.class).create();

    public DriverGremlinPlugin() {
        super(NAME, imports, new DriverConsoleCustomizer());
    }

    private static class DriverConsoleCustomizer implements ConsoleCustomizer {
        @Override
        public org.apache.tinkerpop.gremlin.jsr223.console.RemoteAcceptor getRemoteAcceptor(final GremlinShellEnvironment environment) {
            return new DriverRemoteAcceptor(environment);
        }
    }
}
