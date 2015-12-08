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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV1d0;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class Settings {

    public int port = 8182;

    public List<String> hosts = new ArrayList<>();

    public SerializerSettings serializer = new SerializerSettings();

    public ConnectionPoolSettings connectionPool = new ConnectionPoolSettings();

    public int nioPoolSize = Runtime.getRuntime().availableProcessors();

    public int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;

    public String username = null;

    public String password = null;

    public String jaasEntry = null;

    /**
     * Read configuration from a file into a new {@link Settings} object.
     *
     * @param stream an input stream containing a Gremlin Server YAML configuration
     */
    public static Settings read(final InputStream stream) {
        Objects.requireNonNull(stream);

        final Constructor constructor = new Constructor(Settings.class);
        final TypeDescription settingsDescription = new TypeDescription(Settings.class);
        settingsDescription.putListPropertyType("hosts", String.class);
        settingsDescription.putListPropertyType("serializers", SerializerSettings.class);
        constructor.addTypeDescription(settingsDescription);

        final Yaml yaml = new Yaml(constructor);
        return yaml.loadAs(stream, Settings.class);
    }

    static class ConnectionPoolSettings {
        public boolean enableSsl = false;
        public String trustCertChainFile = null;
        public int minSize = ConnectionPool.MIN_POOL_SIZE;
        public int maxSize = ConnectionPool.MAX_POOL_SIZE;
        public int minSimultaneousUsagePerConnection = ConnectionPool.MIN_SIMULTANEOUS_USAGE_PER_CONNECTION;
        public int maxSimultaneousUsagePerConnection = ConnectionPool.MAX_SIMULTANEOUS_USAGE_PER_CONNECTION;
        public int maxInProcessPerConnection = Connection.MAX_IN_PROCESS;
        public int minInProcessPerConnection = Connection.MIN_IN_PROCESS;
        public int maxWaitForConnection = Connection.MAX_WAIT_FOR_CONNECTION;
        public int maxContentLength = Connection.MAX_CONTENT_LENGTH;
        public int reconnectInterval = Connection.RECONNECT_INTERVAL;
        public int reconnectInitialDelay = Connection.RECONNECT_INITIAL_DELAY;
        public int resultIterationBatchSize = Connection.RESULT_ITERATION_BATCH_SIZE;
        public String channelizer = Channelizer.WebSocketChannelizer.class.getName();

        /**
         * @deprecated as of 3.1.1-incubating, and not replaced as this property was never implemented internally
         * as the way to establish sessions
         */
        @Deprecated
        public String sessionId = null;

        /**
         * @deprecated as of 3.1.1-incubating, and not replaced as this property was never implemented internally
         * as the way to establish sessions
         */
        @Deprecated
        public Optional<String> optionalSessionId() {
            return Optional.ofNullable(sessionId);
        }
    }

    public static class SerializerSettings {
        public String className = GraphSONMessageSerializerV1d0.class.getCanonicalName();
        public Map<String, Object> config = null;

        public MessageSerializer create() throws Exception {
            final Class clazz = Class.forName(className);
            final MessageSerializer serializer = (MessageSerializer) clazz.newInstance();
            Optional.ofNullable(config).ifPresent(c -> serializer.configure(c, null));
            return serializer;
        }
    }
}
