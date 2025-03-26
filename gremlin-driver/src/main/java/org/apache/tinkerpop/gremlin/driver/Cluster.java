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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.Future;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.auth.Auth;
import org.apache.tinkerpop.gremlin.driver.interceptor.PayloadSerializingInterceptor;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A connection to a set of one or more Gremlin Server instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Cluster {
    public static final String SERIALIZER_INTERCEPTOR_NAME = "serializer";
    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private final Manager manager;

    private Cluster(final Builder builder) {
        this.manager = new Manager(builder);
    }

    public synchronized void init() {
        if (!manager.initialized)
            manager.init();
    }

    /**
     * Creates a SessionedClient instance to this {@code Cluster}, meaning requests will be routed to
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
        throw new UnsupportedOperationException("not implemented");
    }

    /**
     * Creates a SessionedClient instance to this {@code Cluster}, meaning requests will be routed to
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
        throw new UnsupportedOperationException("not implemented");
    }

    /**
     * Creates a new {@link Client} based on the settings provided.
     */
    public <T extends Client> T connect() {
        final Client client = new Client.ClusteredClient(this);
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

    public static Builder build(final RequestInterceptor serializingInterceptor) {
        return new Builder(serializingInterceptor);
    }

    public static Builder build(final File configurationFile) throws FileNotFoundException {
        final Settings settings = Settings.read(new FileInputStream(configurationFile));
        return getBuilderFromSettings(settings);
    }

    private static Builder getBuilderFromSettings(final Settings settings) {
        final List<String> addresses = settings.hosts;
        if (addresses.isEmpty())
            throw new IllegalStateException("At least one value must be specified to the hosts setting");

        final Builder builder = new Builder(settings.hosts.get(0))
                .port(settings.port)
                .path(settings.path)
                .enableSsl(settings.connectionPool.enableSsl)
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
                .maxResponseContentLength(settings.connectionPool.maxResponseContentLength)
                .maxWaitForConnection(settings.connectionPool.maxWaitForConnection)
                .maxConnectionPoolSize(settings.connectionPool.maxSize)
                .connectionSetupTimeoutMillis(settings.connectionPool.connectionSetupTimeoutMillis)
                .idleConnectionTimeoutMillis(settings.connectionPool.idleConnectionTimeout)
                .enableUserAgentOnConnect(settings.enableUserAgentOnConnect)
                .bulkResults(settings.bulkResults)
                .validationRequest(settings.connectionPool.validationRequest);

        if (!settings.auth.type.isEmpty()) {
            builder.auth(Auth.from(settings.auth));
        }

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
        return manager.close().thenRun(() -> logger.info("Closed Cluster for hosts [{}]", this));
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
     * Gets the maximum size that the {@link ConnectionPool} can grow.
     */
    public int maxConnectionPoolSize() {
        return manager.connectionPoolSettings.maxSize;
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
     * Gets how long a connection will wait for all pending messages to be returned from the server before closing.
     */
    public int getMaxWaitForClose() {
        return manager.connectionPoolSettings.maxWaitForClose;
    }

    /**
     * Gets the maximum size in bytes of any request received from the server.
     */
    public long getMaxResponseContentLength() {
        return manager.connectionPoolSettings.maxResponseContentLength;
    }

    /**
     * Get time in milliseconds that the driver will allow a channel to not receive read or writes before it automatically closes.
     */
    public long getIdleConnectionTimeout() {
        return manager.connectionPoolSettings.idleConnectionTimeout;
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

    MessageSerializer<?> getSerializer() {
        return manager.serializer;
    }

    List<Pair<String, ? extends RequestInterceptor>> getRequestInterceptors() {
        return manager.interceptors;
    }

    ScheduledExecutorService executor() {
        return manager.executor;
    }

    ScheduledExecutorService hostScheduler() {
        return manager.hostScheduler;
    }

    ScheduledExecutorService connectionScheduler() {
        return manager.connectionScheduler;
    }

    Settings.ConnectionPoolSettings connectionPoolSettings() {
        return manager.connectionPoolSettings;
    }

    LoadBalancingStrategy loadBalancingStrategy() {
        return manager.loadBalancingStrategy;
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

    /**
     * Checks if cluster is configured to send a User Agent header
     * in the web socket handshake
     */
    public boolean isUserAgentOnConnectEnabled() {
        return manager.isUserAgentOnConnectEnabled();
    }

    /**
     * Checks if cluster is configured to bulk results
     */
    public boolean isBulkResultsEnabled() {
        return manager.isBulkResultsEnabled();
    }

    public final static class Builder {
        private static int INTERCEPTOR_NOT_FOUND = -1;

        private final List<InetAddress> addresses = new ArrayList<>();
        private int port = 8182;
        private String path = "/gremlin";
        private MessageSerializer<?> serializer = null;
        private int nioPoolSize = Runtime.getRuntime().availableProcessors();
        private int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        private int maxConnectionPoolSize = ConnectionPool.MAX_POOL_SIZE;
        private int maxWaitForConnection = Connection.MAX_WAIT_FOR_CONNECTION;
        private int maxWaitForClose = Connection.MAX_WAIT_FOR_CLOSE;
        private long maxResponseContentLength = Connection.MAX_RESPONSE_CONTENT_LENGTH;
        private int reconnectInterval = Connection.RECONNECT_INTERVAL;
        private int resultIterationBatchSize = Connection.RESULT_ITERATION_BATCH_SIZE;
        private boolean enableSsl = false;
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
        private LinkedList<Pair<String, ? extends RequestInterceptor>> interceptors = new LinkedList<>();
        private long connectionSetupTimeoutMillis = Connection.CONNECTION_SETUP_TIMEOUT_MILLIS;
        private long idleConnectionTimeoutMillis = Connection.CONNECTION_IDLE_TIMEOUT_MILLIS;
        private boolean enableUserAgentOnConnect = true;
        private boolean bulkResults = false;

        private Builder() {
            addInterceptor(SERIALIZER_INTERCEPTOR_NAME,
                    new PayloadSerializingInterceptor(new GraphBinaryMessageSerializerV4()));
        }

        private Builder(final String address) {
            addContactPoint(address);
            addInterceptor(SERIALIZER_INTERCEPTOR_NAME,
                    new PayloadSerializingInterceptor(new GraphBinaryMessageSerializerV4()));
        }

        private Builder(final RequestInterceptor bodySerializer) {
            addInterceptor(SERIALIZER_INTERCEPTOR_NAME, bodySerializer);
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
        public Builder serializer(final MessageSerializer<?> serializer) {
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
         * this value is not provided and SSL is enabled, the default {@code TrustManager} will be used, which will
         * have a set of common public certificates installed to it.
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
         * The maximum size that the {@link ConnectionPool} can grow.
         */
        public Builder maxConnectionPoolSize(final int maxSize) {
            this.maxConnectionPoolSize = maxSize;
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
         * The amount of time in milliseconds to wait the connection to close before timing out where the default
         * value is 3000. This timeout allows for a delay to occur in waiting for remaining messages that may still
         * be returning from the server while a {@link Client#close()} is called.
         */
        public Builder maxWaitForClose(final int maxWait) {
            this.maxWaitForClose = maxWait;
            return this;
        }

        /**
         * The maximum size in bytes of any response received from the server.
         */
        public Builder maxResponseContentLength(final long maxResponseContentLength) {
            this.maxResponseContentLength = maxResponseContentLength;
            return this;
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
         * Adds a {@link RequestInterceptor} after another one that will allow manipulation of the {@code HttpRequest}
         * prior to its being sent to the server.
         */
        public Builder addInterceptorAfter(final String priorInterceptorName, final String nameOfInterceptor,
                                           final RequestInterceptor interceptor) {
            final int index = getInterceptorIndex(priorInterceptorName);
            if (INTERCEPTOR_NOT_FOUND == index) {
                throw new IllegalArgumentException(priorInterceptorName + " interceptor not found");
            } else if (getInterceptorIndex(nameOfInterceptor) != INTERCEPTOR_NOT_FOUND) {
                throw new IllegalArgumentException(nameOfInterceptor + " interceptor already exists");
            }
            interceptors.add(index + 1, Pair.of(nameOfInterceptor, interceptor));

            return this;
        }

        /**
         * Adds a {@link RequestInterceptor} before another one that will allow manipulation of the {@code HttpRequest}
         * prior to its being sent to the server.
         */
        public Builder addInterceptorBefore(final String subsequentInterceptorName, final String nameOfInterceptor,
                                            final RequestInterceptor interceptor) {
            final int index = getInterceptorIndex(subsequentInterceptorName);
            if (INTERCEPTOR_NOT_FOUND == index) {
                throw new IllegalArgumentException(subsequentInterceptorName + " interceptor not found");
            } else if (getInterceptorIndex(nameOfInterceptor) != INTERCEPTOR_NOT_FOUND) {
                throw new IllegalArgumentException(nameOfInterceptor + " interceptor already exists");
            } else if (index == 0) {
                interceptors.addFirst(Pair.of(nameOfInterceptor, interceptor));
            } else {
                interceptors.add(index - 1, Pair.of(nameOfInterceptor, interceptor));
            }

            return this;
        }

        /**
         * Adds a {@link RequestInterceptor} to the end of the list that will allow manipulation of the
         * {@code HttpRequest} prior to its being sent to the server.
         */
        public Builder addInterceptor(final String name, final RequestInterceptor interceptor) {
            if (getInterceptorIndex(name) != INTERCEPTOR_NOT_FOUND) {
                throw new IllegalArgumentException(name + " interceptor already exists");
            }
            interceptors.add(Pair.of(name, interceptor));
            return this;
        }

        /**
         * Removes a {@link RequestInterceptor} from the list. This can be used to remove the default interceptors that
         * aren't needed.
         */
        public Builder removeInterceptor(final String name) {
            final int index = getInterceptorIndex(name);
            if (index == INTERCEPTOR_NOT_FOUND) {
                throw new IllegalArgumentException(name + " interceptor not found");
            }
            interceptors.remove(index);
            return this;
        }

        private int getInterceptorIndex(final String name) {
            for (int i = 0; i < interceptors.size(); i++) {
                if (interceptors.get(i).getLeft().equals(name)) {
                    return i;
                }
            }

            return INTERCEPTOR_NOT_FOUND;
        }

        /**
         * Adds an Auth {@link RequestInterceptor} to the end of list of interceptors.
         */
        public Builder auth(final Auth auth) {
            addInterceptor(auth.getClass().getSimpleName().toLowerCase() + "-auth", auth);
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

        /**
         * Sets the duration of time in milliseconds provided for connection setup to complete which includes WebSocket
         * handshake and SSL handshake. Beyond this duration an exception would be thrown.
         */
        public Builder connectionSetupTimeoutMillis(final long connectionSetupTimeoutMillis) {
            this.connectionSetupTimeoutMillis = connectionSetupTimeoutMillis;
            return this;
        }

        /**
         * Sets the time in milliseconds that the driver will allow a channel to not receive read or writes before it automatically closes.
         */
        public Builder idleConnectionTimeoutMillis(final long idleConnectionTimeoutMillis) {
            this.idleConnectionTimeoutMillis = idleConnectionTimeoutMillis;
            return this;
        }

        /**
         * Configures whether cluster will send a user agent during
         * web socket handshakes
         * @param enableUserAgentOnConnect true enables the useragent. false disables the useragent.
         */
        public Builder enableUserAgentOnConnect(final boolean enableUserAgentOnConnect) {
            this.enableUserAgentOnConnect = enableUserAgentOnConnect;
            return this;
        }

        /**
         * Configures whether cluster will enable result bulking to optimize performance.
         * @param bulkResults true enables bulking.
         */
        public Builder bulkResults(final boolean bulkResults) {
            this.bulkResults = bulkResults;
            return this;
        }

        List<InetSocketAddress> getContactPoints() {
            return addresses.stream().map(addy -> new InetSocketAddress(addy, port)).collect(Collectors.toList());
        }

        public Cluster create() {
            if (addresses.isEmpty()) addContactPoint("localhost");
            if (null == serializer) serializer = Serializers.GRAPHBINARY_V4.simpleInstance();
            return new Cluster(this);
        }
    }

    static class Factory {
        private final NioEventLoopGroup group;

        public Factory(final int nioPoolSize) {
            final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("gremlin-driver-loop-%d").build();
            group = new NioEventLoopGroup(nioPoolSize, threadFactory);
        }

        Bootstrap createBootstrap() {
            final Bootstrap b = new Bootstrap().group(group);
            b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            return b;
        }

        /**
         * Gracefully shutsdown the event loop and returns the termination future which signals that all jobs are done.
         */
        Future<?> shutdown() {
            // Do not provide a quiet period (default is 2s) to accept more requests. Once we have decided to shutdown,
            // no new requests should be accepted.
            group.shutdownGracefully(/*quiet period*/0, /*timeout*/2, TimeUnit.SECONDS);
            return group.terminationFuture();
        }
    }

    class Manager {
        private final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<>();
        private boolean initialized;
        private final List<InetSocketAddress> contactPoints;
        private final Factory factory;
        private final MessageSerializer<?> serializer;
        private final Settings.ConnectionPoolSettings connectionPoolSettings;
        private final LoadBalancingStrategy loadBalancingStrategy;
        private final Optional<SslContext> sslContextOptional;
        private final Supplier<RequestMessage.Builder> validationRequest;
        private final List<Pair<String, ? extends RequestInterceptor>> interceptors;

        /**
         * Thread pool for requests.
         */
        private final ScheduledThreadPoolExecutor executor;

        /**
         * Thread pool for background work related to the {@link Host}.
         */
        private final ScheduledThreadPoolExecutor hostScheduler;

        /**
         * Thread pool for background work related to the {@link Connection} and {@link ConnectionPool}.
         */
        private final ScheduledThreadPoolExecutor connectionScheduler;

        private final int nioPoolSize;
        private final int workerPoolSize;
        private final int port;
        private final String path;
        private final boolean enableUserAgentOnConnect;
        private final boolean bulkResults;

        private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

        private final List<WeakReference<Client>> openedClients = new ArrayList<>();

        private Manager(final Builder builder) {
            validateBuilder(builder);

            this.loadBalancingStrategy = builder.loadBalancingStrategy;
            this.contactPoints = builder.getContactPoints();
            this.interceptors = builder.interceptors;
            this.enableUserAgentOnConnect = builder.enableUserAgentOnConnect;
            this.bulkResults = builder.bulkResults;

            connectionPoolSettings = new Settings.ConnectionPoolSettings();
            connectionPoolSettings.maxSize = builder.maxConnectionPoolSize;
            connectionPoolSettings.maxWaitForConnection = builder.maxWaitForConnection;
            connectionPoolSettings.maxWaitForClose = builder.maxWaitForClose;
            connectionPoolSettings.maxResponseContentLength = builder.maxResponseContentLength;
            connectionPoolSettings.reconnectInterval = builder.reconnectInterval;
            connectionPoolSettings.resultIterationBatchSize = builder.resultIterationBatchSize;
            connectionPoolSettings.enableSsl = builder.enableSsl;
            connectionPoolSettings.keyStore = builder.keyStore;
            connectionPoolSettings.keyStorePassword = builder.keyStorePassword;
            connectionPoolSettings.trustStore = builder.trustStore;
            connectionPoolSettings.trustStorePassword = builder.trustStorePassword;
            connectionPoolSettings.keyStoreType = builder.keyStoreType;
            connectionPoolSettings.trustStoreType = builder.trustStoreType;
            connectionPoolSettings.sslCipherSuites = builder.sslCipherSuites;
            connectionPoolSettings.sslEnabledProtocols = builder.sslEnabledProtocols;
            connectionPoolSettings.sslSkipCertValidation = builder.sslSkipCertValidation;
            connectionPoolSettings.validationRequest = builder.validationRequest;
            connectionPoolSettings.connectionSetupTimeoutMillis = builder.connectionSetupTimeoutMillis;
            connectionPoolSettings.idleConnectionTimeout = builder.idleConnectionTimeoutMillis;

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

            // the executor above should be reserved for reading/writing background tasks that wont interfere with each
            // other if the thread pool is 1 otherwise tasks may be schedule in such a way as to produce a deadlock
            // as in TINKERPOP-2550. not sure if there is a way to only require the worker pool for all of this. as it
            // sits now the worker pool probably doesn't need to be a scheduled executor type.
            this.hostScheduler = new ScheduledThreadPoolExecutor(contactPoints.size() + 1,
                    new BasicThreadFactory.Builder().namingPattern("gremlin-driver-host-scheduler-%d").build());

            // we distinguish between the hostScheduler and the connectionScheduler because you can end in deadlock
            // if all the possible jobs the driver allows for go to a single thread pool.
            this.connectionScheduler = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                    new BasicThreadFactory.Builder().namingPattern("gremlin-driver-conn-scheduler-%d").build());

            validationRequest = () -> RequestMessage.build(builder.validationRequest);
        }

        private void validateBuilder(final Builder builder) {
            if (builder.maxConnectionPoolSize < 1)
                throw new IllegalArgumentException("maxConnectionPoolSize must be greater than zero");

            if (builder.maxWaitForConnection < 1)
                throw new IllegalArgumentException("maxWaitForConnection must be greater than zero");

            if (builder.maxWaitForClose < 1)
                throw new IllegalArgumentException("maxWaitForClose must be greater than zero");

            if (builder.maxResponseContentLength < 0)
                throw new IllegalArgumentException("maxResponseContentLength must be greater than or equal to zero");

            if (builder.reconnectInterval < 1)
                throw new IllegalArgumentException("reconnectInterval must be greater than zero");

            if (builder.resultIterationBatchSize < 1)
                throw new IllegalArgumentException("resultIterationBatchSize must be greater than zero");

            if (builder.nioPoolSize < 1)
                throw new IllegalArgumentException("nioPoolSize must be greater than zero");

            if (builder.workerPoolSize < 1)
                throw new IllegalArgumentException("workerPoolSize must be greater than zero");

            if (builder.connectionSetupTimeoutMillis < 1)
                throw new IllegalArgumentException("connectionSetupTimeoutMillis must be greater than zero");

            // zero value will disable idle connection detection
            // non-zero will be converted to seconds so any value between 1 and 999 is invalid as it will be less than 1 second
            if (builder.idleConnectionTimeoutMillis != 0 && builder.idleConnectionTimeoutMillis < 1000)
                throw new IllegalArgumentException("idleConnectionTimeoutMillis must be zero or greater than or equal to 1000");

        }

        synchronized void init() {
            if (initialized)
                return;

            initialized = true;

            contactPoints.forEach(address -> {
                final Host host = add(address);
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

            final List<CompletableFuture<Void>> clientCloseFutures = new ArrayList<>(openedClients.size());
            for (WeakReference<Client> openedClient : openedClients) {
                final Client client = openedClient.get();
                if (client != null) {
                    // best to call close() even if the Client is already closing so that we can be sure that
                    // any background client closing operations are included in this shutdown future
                    clientCloseFutures.add(client.closeAsync());
                }
            }

            // when all the clients are fully closed then shutdown the netty event loop. not sure why this needs to
            // block here, but if it doesn't then factory.shutdown() below doesn't seem to want to ever complete.
            // ideally, this should all be async, but i guess it wasn't before this change so just going to leave it
            // for now as this really isn't the focus on this change
            CompletableFuture.allOf(clientCloseFutures.toArray(new CompletableFuture[0])).join();

            final CompletableFuture<Void> closeIt = new CompletableFuture<>();
            // shutdown the event loop. that shutdown can trigger some final jobs to get scheduled so add a listener
            // to the termination event to shutdown remaining thread pools
            factory.shutdown().awaitUninterruptibly().addListener(f -> {
                executor.shutdown();
                hostScheduler.shutdown();
                connectionScheduler.shutdown();
                closeIt.complete(null);
            });

            closeFuture.set(closeIt);

            return closeIt;
        }

        boolean isClosing() {
            return closeFuture.get() != null;
        }

        @Override
        public String toString() {
            return String.join(", ", contactPoints.stream().map(InetSocketAddress::toString).collect(Collectors.<String>toList()));
        }

        /**
         * Checks if cluster is configured to send a User Agent header
         * in the web socket handshake
         */
        public boolean isUserAgentOnConnectEnabled() {
            return enableUserAgentOnConnect;
        }

        /**
         * Checks if cluster is configured to send bulked results
         */
        public boolean isBulkResultsEnabled() {
            return bulkResults;
        }
    }
}
