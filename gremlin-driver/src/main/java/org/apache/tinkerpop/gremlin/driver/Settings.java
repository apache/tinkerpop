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

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Settings for the {@link Cluster} and its related components.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Settings {

    /**
     * The port of the Gremlin Server to connect to which defaults to {@code 8182}. The same port will be applied for
     * all {@link #hosts}.
     */
    public int port = 8182;

    /**
     * The path to the Gremlin service which is defaulted to "/gremlin".
     */
    public String path = "/gremlin";

    /**
     * The list of hosts that the driver will connect to.
     */
    public List<String> hosts = new ArrayList<>();

    /**
     * The serializer that will be used when communicating with the server. Note that serializer settings should match
     * what is available on the server.
     */
    public SerializerSettings serializer = new SerializerSettings();

    /**
     * Settings for connections and connection pool.
     */
    public ConnectionPoolSettings connectionPool = new ConnectionPoolSettings();

    /**
     * Settings for authentication.
     */
    public AuthSettings auth = new AuthSettings();

    /**
     * The size of the thread pool defaulted to the number of available processors.
     */
    public int nioPoolSize = Runtime.getRuntime().availableProcessors();

    /**
     * The number of worker threads defaulted to the number of available processors * 2.
     */
    public int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * Toggles if user agent should be sent in web socket handshakes.
     */
    public boolean enableUserAgentOnConnect = true;

    /**
     * Toggles if result from server is bulked. Default is false.
     */
    public boolean bulkResults = false;

    /**
     * Read configuration from a file into a new {@link Settings} object.
     *
     * @param stream an input stream containing a Gremlin Server YAML configuration
     */
    public static Settings read(final InputStream stream) {
        Objects.requireNonNull(stream);

        final LoaderOptions options = new LoaderOptions();
        final Constructor constructor = new Constructor(Settings.class, options);
        final TypeDescription settingsDescription = new TypeDescription(Settings.class);
        settingsDescription.addPropertyParameters("hosts", String.class);
        settingsDescription.addPropertyParameters("serializer", SerializerSettings.class);
        constructor.addTypeDescription(settingsDescription);

        final Yaml yaml = new Yaml(constructor);
        return yaml.loadAs(stream, Settings.class);
    }

    /**
     * Read configuration from a file into a new {@link Settings} object.
     */
    public static Settings from(final Configuration conf) {
        final Settings settings = new Settings();

        if (conf.containsKey("port"))
            settings.port = conf.getInt("port");

        if (conf.containsKey("nioPoolSize"))
            settings.nioPoolSize = conf.getInt("nioPoolSize");

        if (conf.containsKey("workerPoolSize"))
            settings.workerPoolSize = conf.getInt("workerPoolSize");

        if (conf.containsKey("enableUserAgentOnConnect"))
            settings.enableUserAgentOnConnect = conf.getBoolean("enableUserAgentOnConnect");

        if (conf.containsKey("bulkResults"))
            settings.bulkResults = conf.getBoolean("bulkResults");

        if (conf.containsKey("hosts"))
            settings.hosts = conf.getList("hosts").stream().map(Object::toString).collect(Collectors.toList());

        if (conf.containsKey("serializer.className")) {
            final SerializerSettings serializerSettings = new SerializerSettings();
            final Configuration serializerConf = conf.subset("serializer");

            if (serializerConf.containsKey("className"))
                serializerSettings.className = serializerConf.getString("className");

            final Configuration serializerConfigConf = conf.subset("serializer.config");
            if (IteratorUtils.count(serializerConfigConf.getKeys()) > 0) {
                final Map<String,Object> m = new HashMap<>();
                serializerConfigConf.getKeys().forEachRemaining(name -> m.put(name, serializerConfigConf.getProperty(name)));
                serializerSettings.config = m;
            }
            settings.serializer = serializerSettings;
        }

        final Configuration connectionPoolConf = conf.subset("connectionPool");
        if (IteratorUtils.count(connectionPoolConf.getKeys()) > 0) {
            final ConnectionPoolSettings cpSettings = new ConnectionPoolSettings();

            if (connectionPoolConf.containsKey("enableSsl"))
                cpSettings.enableSsl = connectionPoolConf.getBoolean("enableSsl");

            if (connectionPoolConf.containsKey("keyStore"))
                cpSettings.keyStore = connectionPoolConf.getString("keyStore");

            if (connectionPoolConf.containsKey("keyStorePassword"))
                cpSettings.keyStorePassword = connectionPoolConf.getString("keyStorePassword");

            if (connectionPoolConf.containsKey("keyStoreType"))
                cpSettings.keyStoreType = connectionPoolConf.getString("keyStoreType");

            if (connectionPoolConf.containsKey("trustStore"))
                cpSettings.trustStore = connectionPoolConf.getString("trustStore");

            if (connectionPoolConf.containsKey("trustStorePassword"))
                cpSettings.trustStorePassword = connectionPoolConf.getString("trustStorePassword");

            if (connectionPoolConf.containsKey("trustStoreType"))
                cpSettings.trustStoreType = connectionPoolConf.getString("trustStoreType");

            if (connectionPoolConf.containsKey("sslEnabledProtocols"))
                cpSettings.sslEnabledProtocols = connectionPoolConf.getList("sslEnabledProtocols").stream().map(Object::toString)
                        .collect(Collectors.toList());

            if (connectionPoolConf.containsKey("sslCipherSuites"))
                cpSettings.sslCipherSuites = connectionPoolConf.getList("sslCipherSuites").stream().map(Object::toString)
                        .collect(Collectors.toList());

            if (connectionPoolConf.containsKey("sslSkipCertValidation"))
                cpSettings.sslSkipCertValidation = connectionPoolConf.getBoolean("sslSkipCertValidation");

            if (connectionPoolConf.containsKey("maxSize"))
                cpSettings.maxSize = connectionPoolConf.getInt("maxSize");

            if (connectionPoolConf.containsKey("maxWaitForConnection"))
                cpSettings.maxWaitForConnection = connectionPoolConf.getInt("maxWaitForConnection");

            if (connectionPoolConf.containsKey("maxWaitForClose"))
                cpSettings.maxWaitForClose = connectionPoolConf.getInt("maxWaitForClose");

            if (connectionPoolConf.containsKey("maxResponseContentLength"))
                cpSettings.maxResponseContentLength = connectionPoolConf.getInt("maxResponseContentLength");

            if (connectionPoolConf.containsKey("reconnectInterval"))
                cpSettings.reconnectInterval = connectionPoolConf.getInt("reconnectInterval");

            if (connectionPoolConf.containsKey("resultIterationBatchSize"))
                cpSettings.resultIterationBatchSize = connectionPoolConf.getInt("resultIterationBatchSize");

            if (connectionPoolConf.containsKey("validationRequest"))
                cpSettings.validationRequest = connectionPoolConf.getString("validationRequest");

            if (connectionPoolConf.containsKey("connectionSetupTimeoutMillis"))
                cpSettings.connectionSetupTimeoutMillis = connectionPoolConf.getLong("connectionSetupTimeoutMillis");

            if (connectionPoolConf.containsKey("idleConnectionTimeout"))
                cpSettings.idleConnectionTimeout = connectionPoolConf.getLong("idleConnectionTimeout");

            settings.connectionPool = cpSettings;
        }

        final Configuration authConf = conf.subset("auth");
        if (IteratorUtils.count(authConf.getKeys()) > 0) {
            final AuthSettings authSettings = new AuthSettings();

            if (authConf.containsKey("type"))
                authSettings.type = authConf.getString("type");

            if (authConf.containsKey("username"))
                authSettings.username = authConf.getString("username");

            if (authConf.containsKey("password"))
                authSettings.password = authConf.getString("password");

            settings.auth = authSettings;
        }

        return settings;
    }

    static class ConnectionPoolSettings {
        /**
         * Determines if SSL should be enabled or not. If enabled on the server then it must be enabled on the client.
         */
        public boolean enableSsl = false;

        /**
         * JSSE keystore file path. Similar to setting JSSE property
         * {@code javax.net.ssl.keyStore}.
         */
        public String keyStore;

        /**
         * JSSE keystore password. Similar to setting JSSE property
         * {@code javax.net.ssl.keyStorePassword}.
         */
        public String keyStorePassword;

        /**
         * JSSE truststore file path. Similar to setting JSSE property
         * {@code javax.net.ssl.trustStore}.
         */
        public String trustStore;

        /**
         * JSSE truststore password. Similar to setting JSSE property
         * {@code javax.net.ssl.trustStorePassword}.
         */
        public String trustStorePassword;

        /**
         * JSSE keystore format. 'jks' or 'pkcs12'. Similar to setting JSSE property
         * {@code javax.net.ssl.keyStoreType}.
         */
        public String keyStoreType;

        /**
         * JSSE truststore format. 'jks' or 'pkcs12'. Similar to setting JSSE property
         * {@code javax.net.ssl.trustStoreType}.
         */
        public String trustStoreType;

        /**
         * @see <a href=
         *      "https://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SunJSSE_Protocols">JSSE
         *      Protocols</a>
         */
        public List<String> sslEnabledProtocols = new ArrayList<>();

        /**
         * @see <a href=
         *      "https://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SupportedCipherSuites">Cipher
         *      Suites</a>
         */
        public List<String> sslCipherSuites = new ArrayList<>();

        /**
         * If true, trust all certificates and do not perform any validation.
         */
        public boolean sslSkipCertValidation = false;

        /**
         * The maximum size of a connection pool for a {@link Host}. By default this is set to 128.
         */
        public int maxSize = ConnectionPool.MAX_POOL_SIZE;

        /**
         * The amount of time in milliseconds to wait for a new connection before timing out where the default value
         * is 3000.
         */
        public int maxWaitForConnection = Connection.MAX_WAIT_FOR_CONNECTION;

        /**
         * The amount of time in milliseconds to wait the connection to close before timing out where the default
         * value is 3000. This timeout allows for a delay to occur in waiting for remaining messages that may still
         * be returning from the server while a {@link Client#close()} is called.
         */
        public int maxWaitForClose = Connection.MAX_WAIT_FOR_CLOSE;

        /**
         * The maximum length in bytes of a response message that can be received from the server. The default value is
         * {@link Integer#MAX_VALUE}.
         */
        public long maxResponseContentLength = Connection.MAX_RESPONSE_CONTENT_LENGTH;

        /**
         * The amount of time in milliseconds to wait before trying to reconnect to a dead host. The default value is
         * 1000.
         */
        public int reconnectInterval = Connection.RECONNECT_INTERVAL;

        /**
         * The override value for the size of the result batches to be returned from the server. This value is set to
         * 64 by default.
         */
        public int resultIterationBatchSize = Connection.RESULT_ITERATION_BATCH_SIZE;

        /**
         * A valid Gremlin script that can be used to test remote operations.
         */
        public String validationRequest = "g.inject(0)";

        /**
         * Duration of time in milliseconds provided for connection setup to complete which includes WebSocket
         * handshake and SSL handshake. Beyond this duration an exception would be thrown if the handshake is not
         * complete by then.
         */
        public long connectionSetupTimeoutMillis = Connection.CONNECTION_SETUP_TIMEOUT_MILLIS;

        /**
         * Time in milliseconds that the driver will allow a channel to not receive read or writes before it automatically closes.
         * Note that while this value is to be provided as milliseconds it will resolve to
         * second precision. Set this value to 0 to disable this feature.
         */
        public long idleConnectionTimeout = Connection.CONNECTION_IDLE_TIMEOUT_MILLIS;

    }

    public static class SerializerSettings {
        /**
         * The fully qualified class name of the {@link MessageSerializer} that will be used to communicate with the
         * server. Note that the serializer configured on the client should be supported by the server configuration.
         * By default the setting is configured to {@link GraphBinaryMessageSerializerV4}.
         */
        public String className = GraphBinaryMessageSerializerV4.class.getCanonicalName();

        /**
         * The configuration for the specified serializer with the {@link #className}.
         */
        public Map<String, Object> config = null;

        public MessageSerializer<?> create() throws Exception {
            final Class<?> clazz = Class.forName(className);
            final MessageSerializer<?> serializer = (MessageSerializer<?>) clazz.newInstance();
            Optional.ofNullable(config).ifPresent(c -> serializer.configure(c, null));
            return serializer;
        }
    }

    public static class AuthSettings {
        /**
         * Type of Auth to submit on requests that require authentication.
         */
        public String type = "";
        /**
         * The username to submit on requests that require authentication.
         */
        public String username = null;
        /**
         * The password to submit on requests that require authentication.
         */
        public String password = null;

        /**
         * The region setting for sigv4 authentication.
         */
        public String region = null;

        /**
         * The service name setting for sigv4 authentication.
         */
        public String serviceName = null;
    }
}
