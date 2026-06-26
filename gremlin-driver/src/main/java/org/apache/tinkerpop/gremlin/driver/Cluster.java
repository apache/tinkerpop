/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.driver.auth.Auth;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.driver.remote.HttpRemoteTransaction;
import org.apache.tinkerpop.gremlin.structure.Transaction;
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
import java.net.URISyntaxException;
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
import java.util.Set;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
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
     * Creates a new {@link Client} based on the settings provided.
     */
    public <T extends Client> T connect() {
        final Client client = new Client.ClusteredClient(this);
        manager.trackClient(client);
        return (T) client;
    }

    /**
     * Creates a new {@link Transaction} using the server's default traversal source.
     * The server will bind to "g" by default when no traversal source is specified.
     */
    public RemoteTransaction transact() {
        return transact(null);
    }

    /**
     * Creates a new {@link Transaction} bound to the specified graph or traversal source.
     *
     * @param graphOrTraversalSource the graph/traversal source alias, or null to use the server default
     */
    public RemoteTransaction transact(final String graphOrTraversalSource) {
        // Ensures the shared transaction client and its pools exist before pinning the transaction to a host.
        init();
        final Client.PinnedClient pinnedClient = new Client.PinnedClient(manager.transactionClient);
        return new HttpRemoteTransaction(pinnedClient, graphOrTraversalSource);
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
                .batchSize(settings.connectionPool.batchSize)
                .maxResponseHeaderBytes(settings.connectionPool.maxResponseHeaderBytes)
                .maxWaitForConnection(settings.connectionPool.maxWaitForConnection)
                .maxConnections(settings.connectionPool.maxConnections)
                .connectTimeoutMillis(settings.connectionPool.connectTimeout)
                .idleTimeoutMillis(settings.connectionPool.idleTimeout)
                .keepAliveTimeMillis(settings.connectionPool.keepAliveTime)
                .readTimeoutMillis(settings.connectionPool.readTimeout)
                .compression(settings.connectionPool.compression)
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
    public int maxConnections() {
        return manager.connectionPoolSettings.maxConnections;
    }

    /**
     * Gets the default for the per-request batch size used when the request does not specify one.
     */
    public int getBatchSize() {
        return manager.connectionPoolSettings.batchSize;
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
     * Gets the maximum size in bytes of the HTTP response headers.
     */
    public int getMaxResponseHeaderBytes() {
        return manager.connectionPoolSettings.maxResponseHeaderBytes;
    }

    /**
     * Get time in milliseconds that the driver will allow a channel to not receive read or writes before it automatically closes.
     */
    public long getIdleTimeout() {
        return manager.connectionPoolSettings.idleTimeout;
    }

    /**
     * Gets the duration in milliseconds that bounds TCP connection establishment (including the SSL handshake).
     */
    public int getConnectTimeout() {
        return manager.connectionPoolSettings.connectTimeout;
    }

    /**
     * Gets the idle time in milliseconds before TCP keep-alive probes begin on an otherwise idle connection. A value
     * of {@code 0} disables the feature.
     */
    public long getKeepAliveTime() {
        return manager.connectionPoolSettings.keepAliveTime;
    }

    /**
     * Gets the idle-read timeout in milliseconds that bounds the time between inbound response chunks. A value of
     * {@code 0} disables the feature.
     */
    public long getReadTimeout() {
        return manager.connectionPoolSettings.readTimeout;
    }

    /**
     * Gets the {@link Compression} algorithm negotiated with the server.
     */
    public Compression getCompression() {
        return manager.connectionPoolSettings.compression;
    }

    /**
     * Gets the configured {@link ProxyOptions} or {@code null} if no proxy is configured.
     */
    public ProxyOptions getProxy() {
        return manager.proxyOptions;
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

    public void trackTransaction(final HttpRemoteTransaction tx) {
        manager.trackTransaction(tx);
    }

    public void untrackTransaction(final HttpRemoteTransaction tx) {
        manager.untrackTransaction(tx);
    }

    Factory getFactory() {
        return manager.factory;
    }

    MessageSerializer<?> getSerializer() {
        return manager.serializer;
    }

    List<RequestInterceptor> getRequestInterceptors() {
        return Collections.unmodifiableList(manager.interceptors);
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

    ExecutorService streamingReaderPool() {
        return manager.streamingReaderPool;
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

        private final List<InetAddress> addresses = new ArrayList<>();
        private int port = 8182;
        private String path = "/gremlin";
        private MessageSerializer<?> serializer = null;
        private int nioPoolSize = Runtime.getRuntime().availableProcessors();
        private int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        private int maxConnections = ConnectionPool.MAX_POOL_SIZE;
        private int maxWaitForConnection = Connection.MAX_WAIT_FOR_CONNECTION;
        private int maxWaitForClose = Connection.MAX_WAIT_FOR_CLOSE;
        private int maxResponseHeaderBytes = Connection.MAX_RESPONSE_HEADER_BYTES;
        private int reconnectInterval = Connection.RECONNECT_INTERVAL;
        private int batchSize = Connection.RESULT_ITERATION_BATCH_SIZE;
        private boolean enableSsl = false;
        private String keyStore = null;
        private String keyStorePassword = null;
        private String trustStore = null;
        private String trustStorePassword = null;
        private String keyStoreType = null;
        private String trustStoreType = null;
        private String validationRequest = "g.inject(0)";
        private List<String> sslEnabledProtocols = new ArrayList<>();
        private List<String> sslCipherSuites = new ArrayList<>();
        private boolean sslSkipCertValidation = false;
        private SslContext sslContext = null;
        private LoadBalancingStrategy loadBalancingStrategy = new LoadBalancingStrategy.RoundRobin();
        private List<RequestInterceptor> interceptors = new ArrayList<>();
        private Auth auth = null;
        private int connectTimeout = Connection.CONNECTION_SETUP_TIMEOUT_MILLIS;
        private long idleTimeout = Connection.CONNECTION_IDLE_TIMEOUT_MILLIS;
        private long keepAliveTime = Connection.KEEP_ALIVE_TIME_MILLIS;
        private long readTimeout = Connection.READ_TIMEOUT_MILLIS;
        private Compression compression = Compression.DEFLATE;
        private ProxyOptions proxyOptions = null;
        private boolean enableUserAgentOnConnect = true;
        private boolean bulkResults = false;

        private Builder() {
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
         * Explicitly set the {@code SslContext} to use for SSL connections. When set, all other SSL-related builder
         * settings (key store, trust store, ciphers, protocols, skip-cert-validation) are ignored.
         */
        public Builder ssl(final SslContext sslContext) {
            this.sslContext = sslContext;
            this.enableSsl = true;
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
        public Builder maxConnections(final int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        /**
         * Sets the default for the per-request batch size, used when a request does not specify its own
         * {@code batchSize} via {@link RequestOptions}. Defaults to 64.
         */
        public Builder batchSize(final int size) {
            this.batchSize = size;
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
         * The maximum size in bytes allowed for the HTTP response headers. Defaults to 8192. Exposes the underlying
         * Netty {@code HttpClientCodec} max header size.
         */
        public Builder maxResponseHeaderBytes(final int maxResponseHeaderBytes) {
            this.maxResponseHeaderBytes = maxResponseHeaderBytes;
            return this;
        }

        /**
         * Sets the idle-read timeout in milliseconds that bounds the time between inbound response chunks. The clock
         * is reset on each chunk received, so it bounds idle gaps rather than the total response time. A value of
         * {@code 0} (the default) disables the feature.
         */
        public Builder readTimeoutMillis(final long readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        /**
         * Sets the idle-read timeout that bounds the time between inbound response chunks. Equivalent to
         * {@link #readTimeoutMillis(long)} with the duration expressed as a {@link Duration}. {@link Duration#ZERO}
         * disables the feature.
         */
        public Builder readTimeout(final Duration readTimeout) {
            return readTimeoutMillis(readTimeout.toMillis());
        }

        /**
         * Sets the wire {@link Compression} algorithm negotiated with the server. Defaults to {@link Compression#DEFLATE}
         * (compression on). Set {@link Compression#NONE} to disable.
         */
        public Builder compression(final Compression compression) {
            this.compression = compression == null ? Compression.NONE : compression;
            return this;
        }

        /**
         * Routes connections through the supplied HTTP proxy. A Netty {@code HttpProxyHandler} is inserted into the
         * pipeline before the SSL handler.
         */
        public Builder proxy(final ProxyOptions proxyOptions) {
            this.proxyOptions = proxyOptions;
            return this;
        }

        /**
         * Specify a valid Gremlin script that can be used to test remote operations. This script should be designed
         * to return quickly with the least amount of overhead possible. By default, the script sends
         * {@code g.inject(0)}.
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
         * Sets the list of {@link RequestInterceptor} instances that will be run in order to allow
         * modification of the {@link HttpRequest} prior to its being sent to the server.
         */
        public Builder interceptors(final List<RequestInterceptor> interceptors) {
            this.interceptors = new ArrayList<>(interceptors);
            return this;
        }

        /**
         * Sets the list of {@link RequestInterceptor} instances that will be run in order.
         */
        public Builder interceptors(final RequestInterceptor... interceptors) {
            this.interceptors = new ArrayList<>(List.of(interceptors));
            return this;
        }

        /**
         * Adds an Auth {@link RequestInterceptor} that will always be appended to the end of the interceptor list
         * when the {@link Cluster} is created, regardless of the order in which builder methods are called.
         */
        public Builder auth(final Auth auth) {
            this.auth = auth;
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
         * Configures the endpoint from a single URL string. The URL is parsed and applied to the builder as follows:
         * the scheme selects SSL ({@code https} enables SSL, {@code http} disables it; other or absent schemes leave
         * the current SSL setting unchanged), the host is added as a contact point, an explicit port is applied via
         * {@link #port(int)}, and a non-empty path is applied via {@link #path(String)}. This is a convenience for the
         * common single-host case; use {@link #addContactPoint(String)} / {@link #addContactPoints(String...)} along
         * with {@link #port(int)} and {@link #path(String)} for multi-host configurations.
         * <p>
         * Because this is a single-endpoint convenience, it must not be combined with the multi-host contact point
         * methods: calling it after a contact point has already been added throws {@link IllegalArgumentException}.
         *
         * @param url a full URL such as {@code https://gremlin.example.com:8182/gremlin}
         * @throws IllegalArgumentException if the URL cannot be parsed, has no host, or a contact point already exists
         */
        public Builder url(final String url) {
            if (!this.addresses.isEmpty())
                throw new IllegalArgumentException(
                        "url(String) configures a single endpoint and cannot be combined with addContactPoint(s); " +
                        "use addContactPoint(s) with port(int) and path(String) for multi-host configurations");

            final URI uri;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid url: " + url, e);
            }

            final String host = uri.getHost();
            if (null == host || host.isEmpty())
                throw new IllegalArgumentException("Invalid url, no host could be parsed from: " + url);

            final String scheme = uri.getScheme();
            if (null != scheme) {
                if (scheme.equalsIgnoreCase("https"))
                    this.enableSsl = true;
                else if (scheme.equalsIgnoreCase("http"))
                    this.enableSsl = false;
                else
                    throw new IllegalArgumentException("Invalid url scheme '" + scheme + "', expected http or https in: " + url);
            }

            addContactPoint(host);

            if (uri.getPort() != -1)
                port(uri.getPort());

            final String urlPath = uri.getPath();
            if (null != urlPath && !urlPath.isEmpty())
                path(urlPath);

            return this;
        }

        /**
         * Sets the duration of time in milliseconds that bounds TCP connection establishment (transport setup,
         * including the SSL handshake). Beyond this duration an exception would be thrown. This is a transport
         * establishment timeout, not an HTTP request/response timeout. Defaults to 5000.
         */
        public Builder connectTimeoutMillis(final int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Sets the TCP connection-establishment timeout. Equivalent to {@link #connectTimeoutMillis(int)} with the
         * duration expressed as a {@link Duration}. The value is applied via Netty's int-typed
         * {@code CONNECT_TIMEOUT_MILLIS}, so it must not exceed {@link Integer#MAX_VALUE} milliseconds.
         */
        public Builder connectTimeout(final Duration connectTimeout) {
            final long millis = connectTimeout.toMillis();
            if (millis > Integer.MAX_VALUE)
                throw new IllegalArgumentException("connectTimeout must not exceed " + Integer.MAX_VALUE + " ms");
            return connectTimeoutMillis((int) millis);
        }

        /**
         * Sets the time in milliseconds that the driver will allow a channel to not receive read or writes before it
         * automatically closes.
         */
        public Builder idleTimeoutMillis(final long idleTimeout) {
            this.idleTimeout = idleTimeout;
            return this;
        }

        /**
         * Sets the idle-connection pool timeout. Equivalent to {@link #idleTimeoutMillis(long)} with the duration
         * expressed as a {@link Duration}. {@link Duration#ZERO} disables idle-connection detection.
         */
        public Builder idleTimeout(final Duration idleTimeout) {
            return idleTimeoutMillis(idleTimeout.toMillis());
        }

        /**
         * Sets the idle time in milliseconds before TCP keep-alive probes begin on an otherwise idle connection.
         * When set to a positive value the {@code SO_KEEPALIVE} socket option is enabled and, where supported by the
         * platform/JDK (JDK 11+ on Linux/macOS via {@code TCP_KEEPIDLE}), the per-socket idle time is configured. On
         * platforms/JDKs where the per-socket idle time cannot be set, {@code SO_KEEPALIVE} is still enabled and the
         * OS default idle time is used. Defaults to 30000. Set this value to {@code 0} to disable the feature.
         */
        public Builder keepAliveTimeMillis(final long keepAliveTime) {
            this.keepAliveTime = keepAliveTime;
            return this;
        }

        /**
         * Sets the idle time before TCP keep-alive probes begin. Equivalent to {@link #keepAliveTimeMillis(long)}
         * with the duration expressed as a {@link Duration}. {@link Duration#ZERO} disables the feature.
         */
        public Builder keepAliveTime(final Duration keepAliveTime) {
            return keepAliveTimeMillis(keepAliveTime.toMillis());
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
            if (null != auth) interceptors.add(auth);
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
        private final Set<HttpRemoteTransaction> openTransactions = ConcurrentHashMap.newKeySet();
        private boolean initialized;
        private final List<InetSocketAddress> contactPoints;
        private final Factory factory;
        private final MessageSerializer<?> serializer;
        private final Settings.ConnectionPoolSettings connectionPoolSettings;
        private final LoadBalancingStrategy loadBalancingStrategy;
        private final Optional<SslContext> sslContextOptional;
        private final ProxyOptions proxyOptions;
        private final Supplier<RequestMessage.Builder> validationRequest;
        private final List<RequestInterceptor> interceptors;

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

        /**
         * Cached thread pool for streaming response reader threads. One thread per active streaming response,
         * bounded implicitly by the connection pool size.
         */
        private final ExecutorService streamingReaderPool;

        private final int nioPoolSize;
        private final int workerPoolSize;
        private final int port;
        private final String path;
        private final boolean enableUserAgentOnConnect;
        private final boolean bulkResults;

        private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

        private final List<WeakReference<Client>> openedClients = new ArrayList<>();

        // Single ClusteredClient shared by all transactions. Not tracked in openedClients: it is Cluster-owned and
        // closed explicitly in close(), after the rollbacks that borrow from its pools.
        private Client.ClusteredClient transactionClient;

        private Manager(final Builder builder) {
            validateBuilder(builder);

            this.loadBalancingStrategy = builder.loadBalancingStrategy;
            this.contactPoints = builder.getContactPoints();
            this.interceptors = Collections.unmodifiableList(new ArrayList<>(builder.interceptors));
            this.enableUserAgentOnConnect = builder.enableUserAgentOnConnect;
            this.bulkResults = builder.bulkResults;

            connectionPoolSettings = new Settings.ConnectionPoolSettings();
            connectionPoolSettings.maxConnections = builder.maxConnections;
            connectionPoolSettings.maxWaitForConnection = builder.maxWaitForConnection;
            connectionPoolSettings.maxWaitForClose = builder.maxWaitForClose;
            connectionPoolSettings.maxResponseHeaderBytes = builder.maxResponseHeaderBytes;
            connectionPoolSettings.reconnectInterval = builder.reconnectInterval;
            connectionPoolSettings.batchSize = builder.batchSize;
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
            connectionPoolSettings.connectTimeout = builder.connectTimeout;
            connectionPoolSettings.idleTimeout = builder.idleTimeout;
            connectionPoolSettings.keepAliveTime = builder.keepAliveTime;
            connectionPoolSettings.readTimeout = builder.readTimeout;
            connectionPoolSettings.compression = builder.compression;

            sslContextOptional = Optional.ofNullable(builder.sslContext);
            proxyOptions = builder.proxyOptions;

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

            this.streamingReaderPool = new ThreadPoolExecutor(0, builder.maxConnections * Math.max(contactPoints.size(), 1) * 4,
                    60L, TimeUnit.SECONDS, new SynchronousQueue<>(),
                    new BasicThreadFactory.Builder().namingPattern("gremlin-driver-stream-reader-%d").build());

            validationRequest = () -> RequestMessage.build(builder.validationRequest);
        }

        private void validateBuilder(final Builder builder) {
            if (builder.maxConnections < 1)
                throw new IllegalArgumentException("maxConnections must be greater than zero");

            if (builder.maxWaitForConnection < 1)
                throw new IllegalArgumentException("maxWaitForConnection must be greater than zero");

            if (builder.maxWaitForClose < 1)
                throw new IllegalArgumentException("maxWaitForClose must be greater than zero");

            if (builder.maxResponseHeaderBytes < 1)
                throw new IllegalArgumentException("maxResponseHeaderBytes must be greater than zero");

            if (builder.reconnectInterval < 1)
                throw new IllegalArgumentException("reconnectInterval must be greater than zero");

            if (builder.batchSize < 1)
                throw new IllegalArgumentException("batchSize must be greater than zero");

            if (builder.nioPoolSize < 1)
                throw new IllegalArgumentException("nioPoolSize must be greater than zero");

            if (builder.workerPoolSize < 1)
                throw new IllegalArgumentException("workerPoolSize must be greater than zero");

            if (builder.connectTimeout < 1)
                throw new IllegalArgumentException("connectTimeout must be greater than zero");

            if (builder.readTimeout < 0)
                throw new IllegalArgumentException("readTimeout must be greater than or equal to zero");

            if (builder.keepAliveTime < 0)
                throw new IllegalArgumentException("keepAliveTime must be greater than or equal to zero");

            // zero value will disable idle connection detection
            // non-zero will be converted to seconds so any value between 1 and 999 is invalid as it will be less than 1 second
            if (builder.idleTimeout != 0 && builder.idleTimeout < 1000)
                throw new IllegalArgumentException("idleTimeout must be zero or greater than or equal to 1000");

        }

        synchronized void init() {
            if (initialized)
                return;

            initialized = true;

            contactPoints.forEach(address -> {
                final Host host = add(address);
            });

            // All transactions share one ClusteredClient rather than each transact() standing up (and tearing down on
            // commit/rollback) its own per-host pools. Sharing is safe because transactions borrow a connection per
            // request and return it immediately, so pool occupancy matches normal query traffic. Eager for simplicity;
            // unused connections are reaped by the idle timeout.
            transactionClient = new Client.ClusteredClient(Cluster.this);
            transactionClient.init();
        }

        void trackClient(final Client client) {
            openedClients.add(new WeakReference<>(client));
        }

        void trackTransaction(final HttpRemoteTransaction tx) {
            openTransactions.add(tx);
        }

        void untrackTransaction(final HttpRemoteTransaction tx) {
            openTransactions.remove(tx);
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

            // best-effort rollback of any open transactions before closing
            // snapshot to avoid concurrent modification since rollback() calls untrackTransaction()
            new ArrayList<>(openTransactions).forEach(tx -> {
                try {
                    tx.rollback();
                } catch (Exception e) {
                    logger.warn("Failed to rollback transaction on cluster close", e);
                }
            });
            openTransactions.clear();

            final List<CompletableFuture<Void>> clientCloseFutures = new ArrayList<>(openedClients.size());
            for (WeakReference<Client> openedClient : openedClients) {
                final Client client = openedClient.get();
                if (client != null) {
                    // best to call close() even if the Client is already closing so that we can be sure that
                    // any background client closing operations are included in this shutdown future
                    clientCloseFutures.add(client.closeAsync());
                }
            }

            // Closed here, after the rollbacks above that borrow from its pools (it is intentionally not in
            // openedClients).
            if (transactionClient != null) {
                clientCloseFutures.add(transactionClient.closeAsync());
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
                streamingReaderPool.shutdownNow();
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
