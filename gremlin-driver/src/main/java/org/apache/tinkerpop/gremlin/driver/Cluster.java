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

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A connection to a set of one or more Gremlin Server instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Cluster {
    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private Manager manager;

    private Cluster(final Builder builder) {
        this.manager = new Manager(builder);
    }

    public synchronized void init() {
        if (!manager.initialized)
            manager.init();
    }

    /**
     * Creates a {@link Client.ClusteredClient} instance to this {@code Cluster}, meaning requests will be routed to
     * one or more servers (depending on the cluster configuration), where each request represents the entirety of a
     * transaction.  A commit or rollback (in case of error) is automatically executed at the end of the request.
     * <p/>
     * Note that calling this method does not imply that a connection is made to the server itself at this point.
     * Therefore, if there is only one server specified in the {@code Cluster} and that server is not available an
     * error will not be raised at this point.  Connections get initialized in the {@link Client} when a request is
     * submitted or can be directly initialized via {@link Client#init()}.
     */
    public <T extends Client> T connect() {
        final Client client = new Client.ClusteredClient(this, Client.Settings.build().create());
        manager.trackClient(client);
        return (T) client;
    }

    /**
     * Creates a {@link Client.SessionedClient} instance to this {@code Cluster}, meaning requests will be routed to
     * a single server (randomly selected from the cluster), where the same bindings will be available on each request.
     * Requests are bound to the same thread on the server and thus transactions may extend beyond the bounds of a
     * single request.  The transactions are managed by the user and must be committed or rolled-back manually.
     * <p/>
     * Note that calling this method does not imply that a connection is made to the server itself at this point.
     * Therefore, if there is only one server specified in the {@code Cluster} and that server is not available an
     * error will not be raised at this point.  Connections get initialized in the {@link Client} when a request is
     * submitted or can be directly initialized via {@link Client#init()}.
     *
     * @param sessionId user supplied id for the session which should be unique (a UUID is ideal).
     */
    public <T extends Client> T connect(final String sessionId) {
        return connect(sessionId, false);
    }

    /**
     * Creates a {@link Client.SessionedClient} instance to this {@code Cluster}, meaning requests will be routed to
     * a single server (randomly selected from the cluster), where the same bindings will be available on each request.
     * Requests are bound to the same thread on the server and thus transactions may extend beyond the bounds of a
     * single request.  If {@code manageTransactions} is set to {@code false} then transactions are managed by the
     * user and must be committed or rolled-back manually. When set to {@code true} the transaction is committed or
     * rolled-back at the end of each request.
     * <p/>
     * Note that calling this method does not imply that a connection is made to the server itself at this point.
     * Therefore, if there is only one server specified in the {@code Cluster} and that server is not available an
     * error will not be raised at this point.  Connections get initialized in the {@link Client} when a request is
     * submitted or can be directly initialized via {@link Client#init()}.
     *
     * @param sessionId user supplied id for the session which should be unique (a UUID is ideal).
     * @param manageTransactions enables auto-transactions when set to true
     */
    public <T extends Client> T connect(final String sessionId, final boolean manageTransactions) {
        final Client.SessionSettings sessionSettings = Client.SessionSettings.build()
                .manageTransactions(manageTransactions)
                .sessionId(sessionId).create();
        final Client.Settings settings = Client.Settings.build().useSession(sessionSettings).create();
        return connect(settings);
    }

    /**
     * Creates a new {@link Client} based on the settings provided.
     */
    public <T extends Client> T connect(final Client.Settings settings) {
        final Client client = settings.getSession().isPresent() ? new Client.SessionedClient(this, settings) :
                new Client.ClusteredClient(this, settings);
        manager.trackClient(client);
        return (T) client;
    }

    @Override
    public String toString() {
        return manager.toString();
    }

    public static Builder build() {
        return new Builder();
    }

    public static Builder build(final String address) {
        return new Builder(address);
    }

    public static Builder build(final File configurationFile) throws FileNotFoundException {
        final Settings settings = Settings.read(new FileInputStream(configurationFile));
        return getBuilderFromSettings(settings);
    }

    private static Builder getBuilderFromSettings(final Settings settings) {
        final List<String> addresses = settings.hosts;
        if (addresses.size() == 0)
            throw new IllegalStateException("At least one value must be specified to the hosts setting");

        final Builder builder = new Builder(settings.hosts.get(0))
                .port(settings.port)
                .path(settings.path)
                .enableSsl(settings.connectionPool.enableSsl)
                .trustCertificateChainFile(settings.connectionPool.trustCertChainFile)
                .keepAliveInterval(settings.connectionPool.keepAliveInterval)
                .keyCertChainFile(settings.connectionPool.keyCertChainFile)
                .keyFile(settings.connectionPool.keyFile)
                .keyPassword(settings.connectionPool.keyPassword)
                .keyStore(settings.connectionPool.keyStore)
                .keyStorePassword(settings.connectionPool.keyStorePassword)
                .keyStoreType(settings.connectionPool.keyStoreType)
                .trustStore(settings.connectionPool.trustStore)
                .trustStorePassword(settings.connectionPool.trustStorePassword)
                .trustStoreType(settings.connectionPool.trustStoreType)
                .sslCipherSuites(settings.connectionPool.sslCipherSuites)
                .sslEnabledProtocols(settings.connectionPool.sslEnabledProtocols)
                .sslSkipCertValidation(settings.connectionPool.sslSkipCertValidation)
                .nioPoolSize(settings.nioPoolSize)
                .workerPoolSize(settings.workerPoolSize)
                .reconnectInterval(settings.connectionPool.reconnectInterval)
                .resultIterationBatchSize(settings.connectionPool.resultIterationBatchSize)
                .channelizer(settings.connectionPool.channelizer)
                .maxContentLength(settings.connectionPool.maxContentLength)
                .maxWaitForConnection(settings.connectionPool.maxWaitForConnection)
                .maxInProcessPerConnection(settings.connectionPool.maxInProcessPerConnection)
                .minInProcessPerConnection(settings.connectionPool.minInProcessPerConnection)
                .maxSimultaneousUsagePerConnection(settings.connectionPool.maxSimultaneousUsagePerConnection)
                .minSimultaneousUsagePerConnection(settings.connectionPool.minSimultaneousUsagePerConnection)
                .maxConnectionPoolSize(settings.connectionPool.maxSize)
                .minConnectionPoolSize(settings.connectionPool.minSize)
                .validationRequest(settings.connectionPool.validationRequest);

        if (settings.username != null && settings.password != null)
            builder.credentials(settings.username, settings.password);

        if (settings.jaasEntry != null)
            builder.jaasEntry(settings.jaasEntry);

        if (settings.protocol != null)
            builder.protocol(settings.protocol);

        // the first address was added above in the constructor, so skip it if there are more
        if (addresses.size() > 1)
            addresses.stream().skip(1).forEach(builder::addContactPoint);

        try {
            builder.serializer(settings.serializer.create());
        } catch (Exception ex) {
            throw new IllegalStateException("Could not establish serializer - " + ex.getMessage());
        }

        return builder;
    }

    /**
     * Create a {@code Cluster} with all default settings which will connect to one contact point at {@code localhost}.
     */
    public static Cluster open() {
        return build("localhost").create();
    }

    /**
     * Create a {@code Cluster} from Apache Configurations.
     */
    public static Cluster open(final Configuration conf) {
        return getBuilderFromSettings(Settings.from(conf)).create();
    }

    /**
     * Create a {@code Cluster} using a YAML-based configuration file.
     * First try to read the file from the file system and then from resources.
     */
    public static Cluster open(final String configurationFile) throws Exception {
        final File systemFile = new File(configurationFile);
        if (!systemFile.exists()) {
            final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
            final URL resource = currentClassLoader.getResource(configurationFile);
            final File resourceFile = new File(resource.getFile());
            if (!resourceFile.exists()) {
                throw new IllegalArgumentException(String.format("Configuration file at %s does not exist", configurationFile));
            }

            return build(resourceFile).create();
        }

        return build(systemFile).create();
    }

    public void close() {
        closeAsync().join();
    }

    public CompletableFuture<Void> closeAsync() {
        return manager.close();
    }

    /**
     * Determines if the {@code Cluster} is in the process of closing given a call to {@link #close} or
     * {@link #closeAsync()}.
     */
    public boolean isClosing() {
        return manager.isClosing();
    }

    /**
     * Determines if the {@code Cluster} has completed its closing process after a call to {@link #close} or
     * {@link #closeAsync()}.
     */
    public boolean isClosed() {
        return manager.isClosing() && manager.close().isDone();
    }

    /**
     * Gets the list of hosts that the {@code Cluster} was able to connect to.  A {@link Host} is assumed unavailable
     * until a connection to it is proven to be present.  This will not happen until the {@link Client} submits
     * requests that succeed in reaching a server at the {@link Host} or {@link Client#init()} is called which
     * initializes the {@link ConnectionPool} for the {@link Client} itself.  The number of available hosts returned
     * from this method will change as different servers come on and offline.
     */
    public List<URI> availableHosts() {
        return Collections.unmodifiableList(allHosts().stream()
                .filter(Host::isAvailable)
                .map(Host::getHostUri)
                .collect(Collectors.toList()));
    }

    /**
     * Gets the path to the Gremlin service.
     */
    public String getPath() {
        return manager.path;
    }

    /**
     * Size of the pool for handling request/response operations.
     */
    public int getNioPoolSize() {
        return manager.nioPoolSize;
    }

    /**
     * Size of the pool for handling background work.
     */
    public int getWorkerPoolSize() {
        return manager.workerPoolSize;
    }

    /**
     * Get the {@link MessageSerializer} MIME types supported.
     */
    public String[] getSerializers() {
        return getSerializer().mimeTypesSupported();
    }

    /**
     * Determines if connectivity over SSL is enabled.
     */
    public boolean isSslEnabled() {
        return manager.connectionPoolSettings.enableSsl;
    }

    /**
     * Gets the minimum number of in-flight requests that can occur on a {@link Connection} before it is considered
     * for closing on return to the {@link ConnectionPool}.
     */
    public int getMinInProcessPerConnection() {
        return manager.connectionPoolSettings.minInProcessPerConnection;
    }

    /**
     * Gets the maximum number of in-flight requests that can occur on a {@link Connection}.
     */
    public int getMaxInProcessPerConnection() {
        return manager.connectionPoolSettings.maxInProcessPerConnection;
    }

    /**
     * Gets the maximum number of times that a {@link Connection} can be borrowed from the pool simultaneously.
     */
    public int maxSimultaneousUsagePerConnection() {
        return manager.connectionPoolSettings.maxSimultaneousUsagePerConnection;
    }

    /**
     * Gets the minimum number of times that a {@link Connection} should be borrowed from the pool before it falls
     * under consideration for closing.
     */
    public int minSimultaneousUsagePerConnection() {
        return manager.connectionPoolSettings.minSimultaneousUsagePerConnection;
    }

    /**
     * Gets the maximum size that the {@link ConnectionPool} can grow.
     */
    public int maxConnectionPoolSize() {
        return manager.connectionPoolSettings.maxSize;
    }

    /**
     * Gets the minimum size of the {@link ConnectionPool}.
     */
    public int minConnectionPoolSize() {
        return manager.connectionPoolSettings.minSize;
    }

    /**
     * Gets the override for the server setting that determines how many results are returned per batch.
     */
    public int getResultIterationBatchSize() {
        return manager.connectionPoolSettings.resultIterationBatchSize;
    }

    /**
     * Gets the maximum amount of time to wait for a connection to be borrowed from the connection pool.
     */
    public int getMaxWaitForConnection() {
        return manager.connectionPoolSettings.maxWaitForConnection;
    }

    /**
     * Gets how long a session will stay open assuming the current connection actually is configured for their use.
     *
     * @deprecated As of release 3.3.11, replaced in essence by {@link #getMaxWaitForClose()}.
     */
    public int getMaxWaitForSessionClose() {
        return manager.connectionPoolSettings.maxWaitForSessionClose;
    }

    /**
     * Gets how long a connection will wait for all pending messages to be returned from the server before closing.
     */
    public int getMaxWaitForClose() {
        return manager.connectionPoolSettings.maxWaitForClose;
    }

    /**
     * Gets the maximum size in bytes of any request sent to the server.
     */
    public int getMaxContentLength() {
        return manager.connectionPoolSettings.maxContentLength;
    }

    /**
     * Gets the {@link Channelizer} implementation to use on the client when creating a {@link Connection}.
     */
    public String getChannelizer() {
        return manager.connectionPoolSettings.channelizer;
    }

    /**
     * Gets time in milliseconds to wait between retries when attempting to reconnect to a dead host.
     */
    public int getReconnectInterval() {
        return manager.connectionPoolSettings.reconnectInterval;
    }

    /**
     * Gets time in milliseconds to wait after the last message is sent over a connection before sending a keep-alive
     * message to the server.
     */
    public long getKeepAliveInterval() {
        return manager.connectionPoolSettings.keepAliveInterval;
    }

    /**
     * Specifies the load balancing strategy to use on the client side.
     */
    public Class<? extends LoadBalancingStrategy> getLoadBalancingStrategy() {
        return manager.loadBalancingStrategy.getClass();
    }

    /**
     * Gets the port that the Gremlin Servers will be listening on.
     */
    public int getPort() {
        return manager.port;
    }

    /**
     * Gets a list of all the configured hosts.
     */
    public Collection<Host> allHosts() {
        return Collections.unmodifiableCollection(manager.allHosts());
    }

    Factory getFactory() {
        return manager.factory;
    }

    MessageSerializer getSerializer() {
        return manager.serializer;
    }

    ScheduledExecutorService executor() {
        return manager.executor;
    }

    Settings.ConnectionPoolSettings connectionPoolSettings() {
        return manager.connectionPoolSettings;
    }

    LoadBalancingStrategy loadBalancingStrategy() {
        return manager.loadBalancingStrategy;
    }

    AuthProperties authProperties() {
        return manager.authProps;
    }

    RequestMessage.Builder validationRequest() {
        return manager.validationRequest.get();
    }

    SslContext createSSLContext() throws Exception {
        // if the context is provided then just use that and ignore the other settings
        if (manager.sslContextOptional.isPresent())
            return manager.sslContextOptional.get();

        final SslProvider provider = SslProvider.JDK;
        final Settings.ConnectionPoolSettings connectionPoolSettings = connectionPoolSettings();
        final SslContextBuilder builder = SslContextBuilder.forClient();

        if (connectionPoolSettings.trustCertChainFile != null) {
            logger.warn("Using deprecated SSL trustCertChainFile support");
            builder.trustManager(new File(connectionPoolSettings.trustCertChainFile));
        }

        if (null != connectionPoolSettings.keyCertChainFile && null != connectionPoolSettings.keyFile) {
            logger.warn("Using deprecated SSL keyFile support");
            final File keyCertChainFile = new File(connectionPoolSettings.keyCertChainFile);
            final File keyFile = new File(connectionPoolSettings.keyFile);

            // note that keyPassword may be null here if the keyFile is not
            // password-protected.
            builder.keyManager(keyCertChainFile, keyFile, connectionPoolSettings.keyPassword);
        }

        // Build JSSE SSLContext
        try {

            // Load private key/public cert for client auth
            if (null != connectionPoolSettings.keyStore) {
                final String keyStoreType = null == connectionPoolSettings.keyStoreType ? KeyStore.getDefaultType()
                        : connectionPoolSettings.keyStoreType;
                final KeyStore keystore = KeyStore.getInstance(keyStoreType);
                final char[] password = null == connectionPoolSettings.keyStorePassword ? null
                        : connectionPoolSettings.keyStorePassword.toCharArray();
                try (final InputStream in = new FileInputStream(connectionPoolSettings.keyStore)) {
                    keystore.load(in, password);
                }
                final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keystore, password);
                builder.keyManager(kmf);
            }

            // Load custom truststore
            if (null != connectionPoolSettings.trustStore) {
                final String trustStoreType = null != connectionPoolSettings.trustStoreType ? connectionPoolSettings.trustStoreType
                        : null != connectionPoolSettings.keyStoreType ? connectionPoolSettings.keyStoreType : KeyStore.getDefaultType();
                final KeyStore truststore = KeyStore.getInstance(trustStoreType);
                final char[] password = null == connectionPoolSettings.trustStorePassword ? null
                        : connectionPoolSettings.trustStorePassword.toCharArray();
                try (final InputStream in = new FileInputStream(connectionPoolSettings.trustStore)) {
                    truststore.load(in, password);
                }
                final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(truststore);
                builder.trustManager(tmf);
            }

        } catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException e) {
            logger.error("There was an error enabling SSL.", e);
            return null;
        }

        if (null != connectionPoolSettings.sslCipherSuites && !connectionPoolSettings.sslCipherSuites.isEmpty()) {
            builder.ciphers(connectionPoolSettings.sslCipherSuites);
        }

        if (null != connectionPoolSettings.sslEnabledProtocols && !connectionPoolSettings.sslEnabledProtocols.isEmpty()) {
            builder.protocols(connectionPoolSettings.sslEnabledProtocols.toArray(new String[] {}));
        }

        if (connectionPoolSettings.sslSkipCertValidation) {
            logger.warn("SSL configured with sslSkipCertValidation thus trusts all certificates without verification (not suitable for production)");
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        }

        builder.sslProvider(provider);

        return builder.build();
    }

    public final static class Builder {
        private List<InetAddress> addresses = new ArrayList<>();
        private int port = 8182;
        private String path = "/gremlin";
        private MessageSerializer serializer = Serializers.GRYO_V3D0.simpleInstance();
        private int nioPoolSize = Runtime.getRuntime().availableProcessors();
        private int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        private int minConnectionPoolSize = ConnectionPool.MIN_POOL_SIZE;
        private int maxConnectionPoolSize = ConnectionPool.MAX_POOL_SIZE;
        private int minSimultaneousUsagePerConnection = ConnectionPool.MIN_SIMULTANEOUS_USAGE_PER_CONNECTION;
        private int maxSimultaneousUsagePerConnection = ConnectionPool.MAX_SIMULTANEOUS_USAGE_PER_CONNECTION;
        private int maxInProcessPerConnection = Connection.MAX_IN_PROCESS;
        private int minInProcessPerConnection = Connection.MIN_IN_PROCESS;
        private int maxWaitForConnection = Connection.MAX_WAIT_FOR_CONNECTION;
        private int maxWaitForSessionClose = Connection.MAX_WAIT_FOR_SESSION_CLOSE;
        private int maxWaitForClose = Connection.MAX_WAIT_FOR_CLOSE;
        private int maxContentLength = Connection.MAX_CONTENT_LENGTH;
        private int reconnectInterval = Connection.RECONNECT_INTERVAL;
        private int resultIterationBatchSize = Connection.RESULT_ITERATION_BATCH_SIZE;
        private long keepAliveInterval = Connection.KEEP_ALIVE_INTERVAL;
        private String channelizer = Channelizer.WebSocketChannelizer.class.getName();
        private boolean enableSsl = false;
        private String trustCertChainFile = null;
        private String keyCertChainFile = null;
        private String keyFile = null;
        private String keyPassword = null;
        private String keyStore = null;
        private String keyStorePassword = null;
        private String trustStore = null;
        private String trustStorePassword = null;
        private String keyStoreType = null;
        private String trustStoreType = null;
        private String validationRequest = "''";
        private List<String> sslEnabledProtocols = new ArrayList<>();
        private List<String> sslCipherSuites = new ArrayList<>();
        private boolean sslSkipCertValidation = false;
        private SslContext sslContext = null;
        private LoadBalancingStrategy loadBalancingStrategy = new LoadBalancingStrategy.RoundRobin();
        private AuthProperties authProps = new AuthProperties();

        private Builder() {
            // empty to prevent direct instantiation
        }

        private Builder(final String address) {
            addContactPoint(address);
        }

        /**
         * Size of the pool for handling request/response operations.  Defaults to the number of available processors.
         */
        public Builder nioPoolSize(final int nioPoolSize) {
            this.nioPoolSize = nioPoolSize;
            return this;
        }

        /**
         * Size of the pool for handling background work.  Defaults to the number of available processors multiplied
         * by 2
         */
        public Builder workerPoolSize(final int workerPoolSize) {
            this.workerPoolSize = workerPoolSize;
            return this;
        }

        /**
         * The path to the Gremlin service on the host which is "/gremlin" by default.
         */
        public Builder path(final String path) {
            this.path = path;
            return this;
        }

        /**
         * Set the {@link MessageSerializer} to use given the exact name of a {@link Serializers} enum.  Note that
         * setting this value this way will not allow specific configuration of the serializer itself.  If specific
         * configuration is required * please use {@link #serializer(MessageSerializer)}.
         */
        public Builder serializer(final String mimeType) {
            serializer = Serializers.valueOf(mimeType).simpleInstance();
            return this;
        }

        /**
         * Set the {@link MessageSerializer} to use via the {@link Serializers} enum. If specific configuration is
         * required please use {@link #serializer(MessageSerializer)}.
         */
        public Builder serializer(final Serializers mimeType) {
            serializer = mimeType.simpleInstance();
            return this;
        }

        /**
         * Sets the {@link MessageSerializer} to use.
         */
        public Builder serializer(final MessageSerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        /**
         * Enables connectivity over SSL - note that the server should be configured with SSL turned on for this
         * setting to work properly.
         */
        public Builder enableSsl(final boolean enable) {
            this.enableSsl = enable;
            return this;
        }

        /**
         * Explicitly set the {@code SslContext} for when more flexibility is required in the configuration than is
         * allowed by the {@link Builder}. If this value is set to something other than {@code null} then all other
         * related SSL settings are ignored. The {@link #enableSsl} setting should still be set to {@code true} for
         * this setting to take effect.
         */
        public Builder sslContext(final SslContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        /**
         * File location for a SSL Certificate Chain to use when SSL is enabled. If this value is not provided and
         * SSL is enabled, the default {@link TrustManager} will be used.
         * @deprecated As of release 3.2.10, replaced by {@link #trustStore}
         */
        @Deprecated
        public Builder trustCertificateChainFile(final String certificateChainFile) {
            this.trustCertChainFile = certificateChainFile;
            return this;
        }

        /**
         * Length of time in milliseconds to wait on an idle connection before sending a keep-alive request. This
         * setting is only relevant to {@link Channelizer} implementations that return {@code true} for
         * {@link Channelizer#supportsKeepAlive()}.  Set to zero to disable this feature.
         */
        public Builder keepAliveInterval(final long keepAliveInterval) {
            this.keepAliveInterval = keepAliveInterval;
            return this;
        }

        /**
         * The X.509 certificate chain file in PEM format.
         * @deprecated As of release 3.2.10, replaced by {@link #keyStore}
         */
        @Deprecated
        public Builder keyCertChainFile(final String keyCertChainFile) {
            this.keyCertChainFile = keyCertChainFile;
            return this;
        }

        /**
         * The PKCS#8 private key file in PEM format.
         * @deprecated As of release 3.2.10, replaced by {@link #keyStore}
         */
        @Deprecated
        public Builder keyFile(final String keyFile) {
            this.keyFile = keyFile;
            return this;
        }

        /**
         * The password of the {@link #keyFile}, or {@code null} if it's not password-protected.
         * @deprecated As of release 3.2.10, replaced by {@link #keyStorePassword}
         */
        @Deprecated
        public Builder keyPassword(final String keyPassword) {
            this.keyPassword = keyPassword;
            return this;
        }

        /**
         * The file location of the private key in JKS or PKCS#12 format.
         */
        public Builder keyStore(final String keyStore) {
            this.keyStore = keyStore;
            return this;
        }

        /**
         * The password of the {@link #keyStore}, or {@code null} if it's not password-protected.
         */
        public Builder keyStorePassword(final String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        /**
         * The file location for a SSL Certificate Chain to use when SSL is enabled. If
         * this value is not provided and SSL is enabled, the default {@link TrustManager} will be used.
         */
        public Builder trustStore(final String trustStore) {
            this.trustStore = trustStore;
            return this;
        }

        /**
         * The password of the {@link #trustStore}, or {@code null} if it's not password-protected.
         */
        public Builder trustStorePassword(final String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        /**
         * The format of the {@link #keyStore}, either {@code JKS} or {@code PKCS12}
         */
        public Builder keyStoreType(final String keyStoreType) {
            this.keyStoreType = keyStoreType;
            return this;
        }

        /**
         * The format of the {@link #trustStore}, either {@code JKS} or {@code PKCS12}
         */
        public Builder trustStoreType(final String trustStoreType) {
            this.trustStoreType = trustStoreType;
            return this;
        }

        /**
         * A list of SSL protocols to enable. @see <a href=
         *      "https://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SunJSSE_Protocols">JSSE
         *      Protocols</a>
         */
        public Builder sslEnabledProtocols(final List<String> sslEnabledProtocols) {
            this.sslEnabledProtocols = sslEnabledProtocols;
            return this;
        }

        /**
         * A list of cipher suites to enable. @see <a href=
         *      "https://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SupportedCipherSuites">Cipher
         *      Suites</a>
         */
        public Builder sslCipherSuites(final List<String> sslCipherSuites) {
            this.sslCipherSuites = sslCipherSuites;
            return this;
        }

        /**
         * If true, trust all certificates and do not perform any validation.
         */
        public Builder sslSkipCertValidation(final boolean sslSkipCertValidation) {
            this.sslSkipCertValidation = sslSkipCertValidation;
            return this;
        }

        /**
         * The minimum number of in-flight requests that can occur on a {@link Connection} before it is considered
         * for closing on return to the {@link ConnectionPool}.
         */
        public Builder minInProcessPerConnection(final int minInProcessPerConnection) {
            this.minInProcessPerConnection = minInProcessPerConnection;
            return this;
        }

        /**
         * The maximum number of in-flight requests that can occur on a {@link Connection}. This represents an
         * indication of how busy a {@link Connection} is allowed to be.  This number is linked to the
         * {@link #maxSimultaneousUsagePerConnection} setting, but is slightly different in that it refers to
         * the total number of requests on a {@link Connection}.  In other words, a {@link Connection} might
         * be borrowed once to have multiple requests executed against it.  This number controls the maximum
         * number of requests whereas {@link #maxInProcessPerConnection} controls the times borrowed.
         */
        public Builder maxInProcessPerConnection(final int maxInProcessPerConnection) {
            this.maxInProcessPerConnection = maxInProcessPerConnection;
            return this;
        }

        /**
         * The maximum number of times that a {@link Connection} can be borrowed from the pool simultaneously.
         * This represents an indication of how busy a {@link Connection} is allowed to be.  Set too large and the
         * {@link Connection} may queue requests too quickly, rather than wait for an available {@link Connection}
         * or create a fresh one.  If set too small, the {@link Connection} will show as busy very quickly thus
         * forcing waits for available {@link Connection} instances in the pool when there is more capacity available.
         */
        public Builder maxSimultaneousUsagePerConnection(final int maxSimultaneousUsagePerConnection) {
            this.maxSimultaneousUsagePerConnection = maxSimultaneousUsagePerConnection;
            return this;
        }

        /**
         * The minimum number of times that a {@link Connection} should be borrowed from the pool before it falls
         * under consideration for closing.  If a {@link Connection} is not busy and the
         * {@link #minConnectionPoolSize} is exceeded, then there is no reason to keep that connection open.  Set
         * too large and {@link Connection} that isn't busy will continue to consume resources when it is not being
         * used.  Set too small and {@link Connection} instances will be destroyed when the driver might still be
         * busy.
         */
        public Builder minSimultaneousUsagePerConnection(final int minSimultaneousUsagePerConnection) {
            this.minSimultaneousUsagePerConnection = minSimultaneousUsagePerConnection;
            return this;
        }

        /**
         * The maximum size that the {@link ConnectionPool} can grow.
         */
        public Builder maxConnectionPoolSize(final int maxSize) {
            this.maxConnectionPoolSize = maxSize;
            return this;
        }

        /**
         * The minimum size of the {@link ConnectionPool}.  When the {@link Client} is started, {@link Connection}
         * objects will be initially constructed to this size.
         */
        public Builder minConnectionPoolSize(final int minSize) {
            this.minConnectionPoolSize = minSize;
            return this;
        }

        /**
         * Override the server setting that determines how many results are returned per batch.
         */
        public Builder resultIterationBatchSize(final int size) {
            this.resultIterationBatchSize = size;
            return this;
        }

        /**
         * The maximum amount of time to wait for a connection to be borrowed from the connection pool.
         */
        public Builder maxWaitForConnection(final int maxWait) {
            this.maxWaitForConnection = maxWait;
            return this;
        }

        /**
         * If the connection is using a "session" this setting represents the amount of time in milliseconds to wait
         * for that session to close before timing out where the default value is 3000. Note that the server will
         * eventually clean up dead sessions itself on expiration of the session or during shutdown.
         *
         * @deprecated As of release 3.3.11, replaced in essence by {@link #maxWaitForClose(int)} though behavior
         * described here is still maintained.
         */
        @Deprecated
        public Builder maxWaitForSessionClose(final int maxWait) {
            this.maxWaitForSessionClose = maxWait;
            return this;
        }

        /**
         * The amount of time in milliseconds to wait the connection to close before timing out where the default
         * value is 3000. This timeout allows for a delay to occur in waiting for remaining messages that may still
         * be returning from the server while a {@link Client#close()} is called.
         * <p/>
         * This setting is related to {@link #maxWaitForSessionClose} to some degree. This setting refers to a wait
         * for standard requests (i.e. queries) but the {@link #maxWaitForSessionClose} refers to a wait for the
         * "session close" message that occurs after all standard requests have returned (or timed out). There is
         * generally no need to set {@link #maxWaitForSessionClose} if the server is on 3.3.11 or later as the close
         * of the connection will trigger the close of the session and the release of resources.
         */
        public Builder maxWaitForClose(final int maxWait) {
            this.maxWaitForSessionClose = maxWait;
            return this;
        }

        /**
         * The maximum size in bytes of any request sent to the server.   This number should not exceed the same
         * setting defined on the server.
         */
        public Builder maxContentLength(final int maxContentLength) {
            this.maxContentLength = maxContentLength;
            return this;
        }

        /**
         * Specify the {@link Channelizer} implementation to use on the client when creating a {@link Connection}.
         */
        public Builder channelizer(final String channelizerClass) {
            this.channelizer = channelizerClass;
            return this;
        }

        /**
         * Specify the {@link Channelizer} implementation to use on the client when creating a {@link Connection}.
         */
        public Builder channelizer(final Class channelizerClass) {
            return channelizer(channelizerClass.getCanonicalName());
        }

        /**
         * Specify a valid Gremlin script that can be used to test remote operations. This script should be designed
         * to return quickly with the least amount of overhead possible. By default, the script sends an empty string.
         * If the graph does not support that sort of script because it requires all scripts to include a reference
         * to a graph then a good option might be {@code g.inject()}.
         */
        public Builder validationRequest(final String script) {
            validationRequest = script;
            return this;
        }

        /**
         * Time in milliseconds to wait between retries when attempting to reconnect to a dead host.
         */
        public Builder reconnectInterval(final int interval) {
            this.reconnectInterval = interval;
            return this;
        }

        /**
         * Specifies the load balancing strategy to use on the client side.
         */
        public Builder loadBalancingStrategy(final LoadBalancingStrategy loadBalancingStrategy) {
            this.loadBalancingStrategy = loadBalancingStrategy;
            return this;
        }

        /**
         * Specifies parameters for authentication to Gremlin Server.
         */
        public Builder authProperties(final AuthProperties authProps) {
            this.authProps = authProps;
            return this;
        }

        /**
         * Sets the {@link AuthProperties.Property#USERNAME} and {@link AuthProperties.Property#PASSWORD} properties
         * for authentication to Gremlin Server.
         */
        public Builder credentials(final String username, final String password) {
            authProps = authProps.with(AuthProperties.Property.USERNAME, username).with(AuthProperties.Property.PASSWORD, password);
            return this;
        }

        /**
         * Sets the {@link AuthProperties.Property#PROTOCOL} properties for authentication to Gremlin Server.
         */
        public Builder protocol(final String protocol) {
            this.authProps = authProps.with(AuthProperties.Property.PROTOCOL, protocol);
            return this;
        }

        /**
         * Sets the {@link AuthProperties.Property#JAAS_ENTRY} properties for authentication to Gremlin Server.
         */
        public Builder jaasEntry(final String jaasEntry) {
            this.authProps = authProps.with(AuthProperties.Property.JAAS_ENTRY, jaasEntry);
            return this;
        }

        /**
         * Adds the address of a Gremlin Server to the list of servers a {@link Client} will try to contact to send
         * requests to.  The address should be parseable by {@link InetAddress#getByName(String)}.  That's the only
         * validation performed at this point.  No connection to the host is attempted.
         */
        public Builder addContactPoint(final String address) {
            try {
                this.addresses.add(InetAddress.getByName(address));
                return this;
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }

        /**
         * Add one or more the addresses of a Gremlin Servers to the list of servers a {@link Client} will try to
         * contact to send requests to.  The address should be parseable by {@link InetAddress#getByName(String)}.
         * That's the only validation performed at this point.  No connection to the host is attempted.
         */
        public Builder addContactPoints(final String... addresses) {
            for (String address : addresses)
                addContactPoint(address);
            return this;
        }

        /**
         * Sets the port that the Gremlin Servers will be listening on.
         */
        public Builder port(final int port) {
            this.port = port;
            return this;
        }

        List<InetSocketAddress> getContactPoints() {
            return addresses.stream().map(addy -> new InetSocketAddress(addy, port)).collect(Collectors.toList());
        }

        public Cluster create() {
            if (addresses.size() == 0) addContactPoint("localhost");
            return new Cluster(this);
        }
    }

    static class Factory {
        private final EventLoopGroup group;

        public Factory(final int nioPoolSize) {
            final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("gremlin-driver-loop-%d").build();
            group = new NioEventLoopGroup(nioPoolSize, threadFactory);
        }

        Bootstrap createBootstrap() {
            final Bootstrap b = new Bootstrap().group(group);
            b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            return b;
        }

        void shutdown() {
            group.shutdownGracefully().awaitUninterruptibly();
        }
    }

    class Manager {
        private final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<>();
        private boolean initialized;
        private final List<InetSocketAddress> contactPoints;
        private final Factory factory;
        private final MessageSerializer serializer;
        private final Settings.ConnectionPoolSettings connectionPoolSettings;
        private final LoadBalancingStrategy loadBalancingStrategy;
        private final AuthProperties authProps;
        private final Optional<SslContext> sslContextOptional;
        private final Supplier<RequestMessage.Builder> validationRequest;

        private final ScheduledThreadPoolExecutor executor;

        private final int nioPoolSize;
        private final int workerPoolSize;
        private final int port;
        private final String path;

        private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

        private final List<WeakReference<Client>> openedClients = new ArrayList<>();

        private Manager(final Builder builder) {
            validateBuilder(builder);

            this.loadBalancingStrategy = builder.loadBalancingStrategy;
            this.authProps = builder.authProps;
            this.contactPoints = builder.getContactPoints();

            connectionPoolSettings = new Settings.ConnectionPoolSettings();
            connectionPoolSettings.maxInProcessPerConnection = builder.maxInProcessPerConnection;
            connectionPoolSettings.minInProcessPerConnection = builder.minInProcessPerConnection;
            connectionPoolSettings.maxSimultaneousUsagePerConnection = builder.maxSimultaneousUsagePerConnection;
            connectionPoolSettings.minSimultaneousUsagePerConnection = builder.minSimultaneousUsagePerConnection;
            connectionPoolSettings.maxSize = builder.maxConnectionPoolSize;
            connectionPoolSettings.minSize = builder.minConnectionPoolSize;
            connectionPoolSettings.maxWaitForConnection = builder.maxWaitForConnection;
            connectionPoolSettings.maxWaitForSessionClose = builder.maxWaitForSessionClose;
            connectionPoolSettings.maxWaitForClose = builder.maxWaitForClose;
            connectionPoolSettings.maxContentLength = builder.maxContentLength;
            connectionPoolSettings.reconnectInterval = builder.reconnectInterval;
            connectionPoolSettings.resultIterationBatchSize = builder.resultIterationBatchSize;
            connectionPoolSettings.enableSsl = builder.enableSsl;
            connectionPoolSettings.trustCertChainFile = builder.trustCertChainFile;
            connectionPoolSettings.keyCertChainFile = builder.keyCertChainFile;
            connectionPoolSettings.keyFile = builder.keyFile;
            connectionPoolSettings.keyPassword = builder.keyPassword;
            connectionPoolSettings.keyStore = builder.keyStore;
            connectionPoolSettings.keyStorePassword = builder.keyStorePassword;
            connectionPoolSettings.trustStore = builder.trustStore;
            connectionPoolSettings.trustStorePassword = builder.trustStorePassword;
            connectionPoolSettings.keyStoreType = builder.keyStoreType;
            connectionPoolSettings.trustStoreType = builder.trustStoreType;
            connectionPoolSettings.sslCipherSuites = builder.sslCipherSuites;
            connectionPoolSettings.sslEnabledProtocols = builder.sslEnabledProtocols;
            connectionPoolSettings.sslSkipCertValidation = builder.sslSkipCertValidation;
            connectionPoolSettings.keepAliveInterval = builder.keepAliveInterval;
            connectionPoolSettings.channelizer = builder.channelizer;
            connectionPoolSettings.validationRequest = builder.validationRequest;

            sslContextOptional = Optional.ofNullable(builder.sslContext);

            nioPoolSize = builder.nioPoolSize;
            workerPoolSize = builder.workerPoolSize;
            port = builder.port;
            path = builder.path;

            this.factory = new Factory(builder.nioPoolSize);
            this.serializer = builder.serializer;
            
            this.executor = new ScheduledThreadPoolExecutor(builder.workerPoolSize,
                    new BasicThreadFactory.Builder().namingPattern("gremlin-driver-worker-%d").build());
            this.executor.setRemoveOnCancelPolicy(true);

            validationRequest = () -> RequestMessage.build(Tokens.OPS_EVAL).add(Tokens.ARGS_GREMLIN, builder.validationRequest);
        }

        private void validateBuilder(final Builder builder) {
            if (builder.minInProcessPerConnection < 0)
                throw new IllegalArgumentException("minInProcessPerConnection must be greater than or equal to zero");

            if (builder.maxInProcessPerConnection < 1)
                throw new IllegalArgumentException("maxInProcessPerConnection must be greater than zero");

            if (builder.minInProcessPerConnection > builder.maxInProcessPerConnection)
                throw new IllegalArgumentException("maxInProcessPerConnection cannot be less than minInProcessPerConnection");

            if (builder.minSimultaneousUsagePerConnection < 0)
                throw new IllegalArgumentException("minSimultaneousUsagePerConnection must be greater than or equal to zero");

            if (builder.maxSimultaneousUsagePerConnection < 1)
                throw new IllegalArgumentException("maxSimultaneousUsagePerConnection must be greater than zero");

            if (builder.minSimultaneousUsagePerConnection > builder.maxSimultaneousUsagePerConnection)
                throw new IllegalArgumentException("maxSimultaneousUsagePerConnection cannot be less than minSimultaneousUsagePerConnection");

            if (builder.minConnectionPoolSize < 0)
                throw new IllegalArgumentException("minConnectionPoolSize must be greater than or equal to zero");

            if (builder.maxConnectionPoolSize < 1)
                throw new IllegalArgumentException("maxConnectionPoolSize must be greater than zero");

            if (builder.minConnectionPoolSize > builder.maxConnectionPoolSize)
                throw new IllegalArgumentException("maxConnectionPoolSize cannot be less than minConnectionPoolSize");

            if (builder.maxWaitForConnection < 1)
                throw new IllegalArgumentException("maxWaitForConnection must be greater than zero");

            if (builder.maxWaitForSessionClose < 1)
                throw new IllegalArgumentException("maxWaitForSessionClose must be greater than zero");

            if (builder.maxWaitForClose < 1)
                throw new IllegalArgumentException("maxWaitForClose must be greater than zero");

            if (builder.maxContentLength < 1)
                throw new IllegalArgumentException("maxContentLength must be greater than zero");

            if (builder.reconnectInterval < 1)
                throw new IllegalArgumentException("reconnectInterval must be greater than zero");

            if (builder.resultIterationBatchSize < 1)
                throw new IllegalArgumentException("resultIterationBatchSize must be greater than zero");

            if (builder.nioPoolSize < 1)
                throw new IllegalArgumentException("nioPoolSize must be greater than zero");

            if (builder.workerPoolSize < 1)
                throw new IllegalArgumentException("workerPoolSize must be greater than zero");

            try {
                Class.forName(builder.channelizer);
            } catch (Exception ex) {
                throw new IllegalArgumentException("The channelizer specified [" + builder.channelizer +
                        "] could not be instantiated - it should be the fully qualified classname of a Channelizer implementation available on the classpath", ex);
            }
        }

        synchronized void init() {
            if (initialized)
                return;

            initialized = true;

            contactPoints.forEach(address -> {
                final Host host = add(address);
                if (host != null)
                    host.makeAvailable();
            });
        }

        void trackClient(final Client client) {
            openedClients.add(new WeakReference<>(client));
        }

        public Host add(final InetSocketAddress address) {
            final Host newHost = new Host(address, Cluster.this);
            final Host previous = hosts.putIfAbsent(address, newHost);
            return previous == null ? newHost : null;
        }

        Collection<Host> allHosts() {
            return hosts.values();
        }

        synchronized CompletableFuture<Void> close() {
            // this method is exposed publicly in both blocking and non-blocking forms.
            if (closeFuture.get() != null)
                return closeFuture.get();

            for (WeakReference<Client> openedClient : openedClients) {
                final Client client = openedClient.get();
                if (client != null && !client.isClosing()) {
                    client.close();
                }
            }

            final CompletableFuture<Void> closeIt = new CompletableFuture<>();
            closeFuture.set(closeIt);

            executor().submit(() -> {
                factory.shutdown();
                closeIt.complete(null);
            });

            // Prevent the executor from accepting new tasks while still allowing enqueued tasks to complete
            executor.shutdown();

            return closeIt;
        }

        boolean isClosing() {
            return closeFuture.get() != null;
        }

        @Override
        public String toString() {
            return String.join(", ", contactPoints.stream().map(InetSocketAddress::toString).collect(Collectors.<String>toList()));
        }
    }
}
