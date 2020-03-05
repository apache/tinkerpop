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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
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
final class Settings {

    /**
     * The port of the Gremlin Server to connect to which defaults to {@code 8192}. The same port will be applied for
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
     * The size of the thread pool defaulted to the number of available processors.
     */
    public int nioPoolSize = Runtime.getRuntime().availableProcessors();

    /**
     * The number of worker threads defaulted to the number of available processors * 2.
     */
    public int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * The username to submit on requests that require authentication.
     */
    public String username = null;

    /**
     * The password to submit on requests that require authentication.
     */
    public String password = null;

    /**
     * The JAAS to submit on requests that require authentication.
     */
    public String jaasEntry = null;

    /**
     * The JAAS protocol to submit on requests that require authentication.
     */
    public String protocol = null;

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

        if (conf.containsKey("username"))
            settings.username = conf.getString("username");

        if (conf.containsKey("password"))
            settings.password = conf.getString("password");

        if (conf.containsKey("jaasEntry"))
            settings.jaasEntry = conf.getString("jaasEntry");

        if (conf.containsKey("protocol"))
            settings.protocol = conf.getString("protocol");

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
                serializerConfigConf.getKeys().forEachRemaining(name -> {
                    m.put(name, serializerConfigConf.getProperty(name));
                });
                serializerSettings.config = m;
            }
            settings.serializer = serializerSettings;
        }

        final Configuration connectionPoolConf = conf.subset("connectionPool");
        if (IteratorUtils.count(connectionPoolConf.getKeys()) > 0) {
            final ConnectionPoolSettings cpSettings = new ConnectionPoolSettings();

            if (connectionPoolConf.containsKey("channelizer"))
                cpSettings.channelizer = connectionPoolConf.getString("channelizer");

            if (connectionPoolConf.containsKey("enableSsl"))
                cpSettings.enableSsl = connectionPoolConf.getBoolean("enableSsl");

            if (connectionPoolConf.containsKey("keyCertChainFile"))
                cpSettings.keyCertChainFile = connectionPoolConf.getString("keyCertChainFile");

            if (connectionPoolConf.containsKey("keyFile"))
                cpSettings.keyFile = connectionPoolConf.getString("keyFile");

            if (connectionPoolConf.containsKey("keyPassword"))
                cpSettings.keyPassword = connectionPoolConf.getString("keyPassword");

            if (connectionPoolConf.containsKey("trustCertChainFile"))
                cpSettings.trustCertChainFile = connectionPoolConf.getString("trustCertChainFile");

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

            if (connectionPoolConf.containsKey("minSize"))
                cpSettings.minSize = connectionPoolConf.getInt("minSize");

            if (connectionPoolConf.containsKey("maxSize"))
                cpSettings.maxSize = connectionPoolConf.getInt("maxSize");

            if (connectionPoolConf.containsKey("minSimultaneousUsagePerConnection"))
                cpSettings.minSimultaneousUsagePerConnection = connectionPoolConf.getInt("minSimultaneousUsagePerConnection");

            if (connectionPoolConf.containsKey("maxSimultaneousUsagePerConnection"))
                cpSettings.maxSimultaneousUsagePerConnection = connectionPoolConf.getInt("maxSimultaneousUsagePerConnection");

            if (connectionPoolConf.containsKey("maxInProcessPerConnection"))
                cpSettings.maxInProcessPerConnection = connectionPoolConf.getInt("maxInProcessPerConnection");

            if (connectionPoolConf.containsKey("minInProcessPerConnection"))
                cpSettings.minInProcessPerConnection = connectionPoolConf.getInt("minInProcessPerConnection");

            if (connectionPoolConf.containsKey("maxWaitForConnection"))
                cpSettings.maxWaitForConnection = connectionPoolConf.getInt("maxWaitForConnection");

            if (connectionPoolConf.containsKey("maxWaitForSessionClose"))
                cpSettings.maxWaitForSessionClose = connectionPoolConf.getInt("maxWaitForSessionClose");

            if (connectionPoolConf.containsKey("maxWaitForClose"))
                cpSettings.maxWaitForClose = connectionPoolConf.getInt("maxWaitForClose");

            if (connectionPoolConf.containsKey("maxContentLength"))
                cpSettings.maxContentLength = connectionPoolConf.getInt("maxContentLength");

            if (connectionPoolConf.containsKey("reconnectInterval"))
                cpSettings.reconnectInterval = connectionPoolConf.getInt("reconnectInterval");

            if (connectionPoolConf.containsKey("resultIterationBatchSize"))
                cpSettings.resultIterationBatchSize = connectionPoolConf.getInt("resultIterationBatchSize");

            if (connectionPoolConf.containsKey("keepAliveInterval"))
                cpSettings.keepAliveInterval = connectionPoolConf.getLong("keepAliveInterval");

            if (connectionPoolConf.containsKey("validationRequest"))
                cpSettings.validationRequest = connectionPoolConf.getString("validationRequest");

            settings.connectionPool = cpSettings;
        }

        return settings;
    }

    static class ConnectionPoolSettings {
        /**
         * Determines if SSL should be enabled or not. If enabled on the server then it must be enabled on the client.
         */
        public boolean enableSsl = false;

        /**
         * The trusted certificate in PEM format.
         * @deprecated As of release 3.2.10, replaced by {@link #trustStore}
         */
        @Deprecated
        public String trustCertChainFile = null;

        /**
         * The X.509 certificate chain file in PEM format.
         * @deprecated As of release 3.2.10, replaced by {@link #keyStore}
         */
        @Deprecated
        public String keyCertChainFile = null;

        /**
         * The PKCS#8 private key file in PEM format.
         * @deprecated As of release 3.2.10, replaced by {@link #keyStore}
         */
        @Deprecated
        public String keyFile = null;

        /**
         * The password of the {@link #keyFile}, or {@code null} if it's not password-protected.
         * @deprecated As of release 3.2.10, replaced by {@link #keyStorePassword}
         */
        @Deprecated
        public String keyPassword = null;

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
         * The minimum size of a connection pool for a {@link Host}. By default this is set to 2.
         */
        public int minSize = ConnectionPool.MIN_POOL_SIZE;

        /**
         * The maximum size of a connection pool for a {@link Host}. By default this is set to 8.
         */
        public int maxSize = ConnectionPool.MAX_POOL_SIZE;

        /**
         * Length of time in milliseconds to wait on an idle connection before sending a keep-alive request. This
         * setting is only relevant to {@link Channelizer} implementations that return {@code true} for
         * {@link Channelizer#supportsKeepAlive()}. Set to zero to disable this feature.
         */
        public long keepAliveInterval = Connection.KEEP_ALIVE_INTERVAL;

        /**
         * A connection under low use can be destroyed. This setting determines the threshold for determining when
         * that connection can be released and is defaulted to 8.
         */
        public int minSimultaneousUsagePerConnection = ConnectionPool.MIN_SIMULTANEOUS_USAGE_PER_CONNECTION;

        /**
         * If a connection is over used, then it might mean that is necessary to expand the pool by adding a new
         * connection.  This setting determines the threshold for a connections over use and is defaulted to 16
         */
        public int maxSimultaneousUsagePerConnection = ConnectionPool.MAX_SIMULTANEOUS_USAGE_PER_CONNECTION;

        /**
         * The maximum number of requests in flight on a connection where the default is 4.
         */
        public int maxInProcessPerConnection = Connection.MAX_IN_PROCESS;

        /**
         * A connection has available in-process requests which is calculated by subtracting the number of current
         * in-flight requests on a connection and subtracting that from the {@link #maxInProcessPerConnection}. When
         * that number drops below this configuration setting, the connection is recommended for replacement. The
         * default for this setting is 1.
         */
        public int minInProcessPerConnection = Connection.MIN_IN_PROCESS;

        /**
         * The amount of time in milliseconds to wait for a new connection before timing out where the default value
         * is 3000.
         */
        public int maxWaitForConnection = Connection.MAX_WAIT_FOR_CONNECTION;

        /**
         * If the connection is using a "session" this setting represents the amount of time in milliseconds to wait
         * for that session to close before timing out where the default value is 3000. Note that the server will
         * eventually clean up dead sessions itself on expiration of the session or during shutdown.
         *
         * @deprecated As of release 3.3.11, replaced in essence by {@link #maxWaitForClose}.
         */
        @Deprecated
        public int maxWaitForSessionClose = Connection.MAX_WAIT_FOR_SESSION_CLOSE;

        /**
         * The amount of time in milliseconds to wait the connection to close before timing out where the default
         * value is 3000. This timeout allows for a delay to occur in waiting for remaining messages that may still
         * be returning from the server while a {@link Client#close()} is called.
         */
        public int maxWaitForClose = Connection.MAX_WAIT_FOR_CLOSE;

        /**
         * The maximum length in bytes that a message can be sent to the server. This number can be no greater than
         * the setting of the same name in the server configuration. The default value is 65536.
         */
        public int maxContentLength = Connection.MAX_CONTENT_LENGTH;

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
         * The constructor for the channel that connects to the server. This value should be the fully qualified
         * class name of a Gremlin Driver {@link Channelizer} implementation.  By default this value is set to
         * {@link Channelizer.WebSocketChannelizer}.
         */
        public String channelizer = Channelizer.WebSocketChannelizer.class.getName();

        /**
         * A valid Gremlin script that can be used to test remote operations.
         */
        public String validationRequest = "''";
    }

    public static class SerializerSettings {
        /**
         * The fully qualified class name of the {@link MessageSerializer} that will be used to communicate with the
         * server. Note that the serializer configured on the client should be supported by the server configuration.
         */
        public String className = GryoMessageSerializerV3d0.class.getCanonicalName();

        /**
         * The configuration for the specified serializer with the {@link #className}.
         */
        public Map<String, Object> config = null;

        public MessageSerializer create() throws Exception {
            final Class clazz = Class.forName(className);
            final MessageSerializer serializer = (MessageSerializer) clazz.newInstance();
            Optional.ofNullable(config).ifPresent(c -> serializer.configure(c, null));
            return serializer;
        }
    }
}
