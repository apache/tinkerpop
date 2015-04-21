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
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * A bunch of Gremlin Server instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Cluster {

    private Manager manager;

    private Cluster(final List<InetSocketAddress> contactPoints, final MessageSerializer serializer,
                    final int nioPoolSize, final int workerPoolSize,
                    final Settings.ConnectionPoolSettings connectionPoolSettings,
                    final LoadBalancingStrategy loadBalancingStrategy) {
        this.manager = new Manager(contactPoints, serializer, nioPoolSize, workerPoolSize, connectionPoolSettings, loadBalancingStrategy);
    }

    public synchronized void init() {
        if (!manager.initialized)
            manager.init();
    }

    /**
     * Creates a {@link Client.ClusteredClient} instance to this {@code Cluster}, meaning requests will be routed to
     * one or more servers (depending on the cluster configuration), where each request represents the entirety of a
     * transaction.  A commit or rollback (in case of error) is automatically executed at the end of the request.
     */
    public Client connect() {
        return new Client.ClusteredClient(this);
    }

    /**
     * Creates a {@link Client.SessionedClient} instance to this {@code Cluster}, meaning requests will be routed to
     * a single server (randomly selected from the cluster), where the same bindings will be available on each request.
     * Requests are bound to the same thread on the server and thus transactions may extend beyond the bounds of a
     * single request.  The transactions are managed by the user and must be committed or rolledback manually.
     *
     * @param sessionId user supplied id for the session which should be unique (a UUID is ideal).
     */
    public Client connect(final String sessionId) {
        if (null == sessionId || sessionId.isEmpty())
            throw new IllegalArgumentException("sessionId cannot be null or empty");
        return new Client.SessionedClient(this, sessionId);
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
        final List<String> addresses = settings.hosts;
        if (addresses.size() == 0)
            throw new IllegalStateException("At least one value must be specified to the hosts setting");

        final Builder builder = new Builder(settings.hosts.get(0))
                .port(settings.port)
                .nioPoolSize(settings.nioPoolSize)
                .workerPoolSize(settings.workerPoolSize)
                .maxInProcessPerConnection(settings.connectionPool.maxInProcessPerConnection)
                .maxSimultaneousRequestsPerConnection(settings.connectionPool.maxSimultaneousRequestsPerConnection)
                .minSimultaneousRequestsPerConnection(settings.connectionPool.minSimultaneousRequestsPerConnection)
                .maxConnectionPoolSize(settings.connectionPool.maxSize)
                .minConnectionPoolSize(settings.connectionPool.minSize);

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
     * Create a {@code Cluster} using a YAML-based configuration file.
     */
    public static Cluster open(final String configurationFile) throws Exception {
        final File file = new File(configurationFile);
        if (!file.exists())
            throw new IllegalArgumentException(String.format("Configuration file at %s does not exist", configurationFile));

        return build(file).create();
    }

    public void close() {
        closeAsync().join();
    }

    public CompletableFuture<Void> closeAsync() {
        return manager.close();
    }

    public List<URI> availableHosts() {
        return Collections.unmodifiableList(getClusterInfo().allHosts().stream()
                .filter(Host::isAvailable)
                .map(Host::getHostUri)
                .collect(Collectors.toList()));
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

    ClusterInfo getClusterInfo() {
        return manager.clusterInfo;
    }

    public static class Builder {
        private List<InetAddress> addresses = new ArrayList<>();
        private int port = 8182;
        private MessageSerializer serializer = Serializers.GRYO_V1D0.simpleInstance();
        private int nioPoolSize = Runtime.getRuntime().availableProcessors();
        private int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        private int minConnectionPoolSize = ConnectionPool.MIN_POOL_SIZE;
        private int maxConnectionPoolSize = ConnectionPool.MAX_POOL_SIZE;
        private int minSimultaneousRequestsPerConnection = ConnectionPool.MIN_SIMULTANEOUS_REQUESTS_PER_CONNECTION;
        private int maxSimultaneousRequestsPerConnection = ConnectionPool.MAX_SIMULTANEOUS_REQUESTS_PER_CONNECTION;
        private int maxInProcessPerConnection = Connection.MAX_IN_PROCESS;
        private int minInProcessPerConnection = Connection.MIN_IN_PROCESS;
        private int maxWaitForConnection = Connection.MAX_WAIT_FOR_CONNECTION;
        private int maxContentLength = Connection.MAX_CONTENT_LENGTH;
        private int reconnectInitialDelay = Connection.RECONNECT_INITIAL_DELAY;
        private int reconnectInterval = Connection.RECONNECT_INTERVAL;
        private int resultIterationBatchSize = Connection.RESULT_ITERATION_BATCH_SIZE;
        private boolean enableSsl = false;
        private LoadBalancingStrategy loadBalancingStrategy = new LoadBalancingStrategy.RoundRobin();

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
            if (nioPoolSize < 1) throw new IllegalArgumentException("The workerPoolSize must be greater than zero");
            this.nioPoolSize = nioPoolSize;
            return this;
        }

        /**
         * Size of the pool for handling background work.  Defaults to the number of available processors multiplied
         * by 2
         */
        public Builder workerPoolSize(final int workerPoolSize) {
            if (workerPoolSize < 1) throw new IllegalArgumentException("The workerPoolSize must be greater than zero");
            this.workerPoolSize = workerPoolSize;
            return this;
        }

        public Builder serializer(final String mimeType) {
            serializer = Serializers.valueOf(mimeType).simpleInstance();
            return this;
        }

        public Builder serializer(final Serializers mimeType) {
            serializer = mimeType.simpleInstance();
            return this;
        }

        public Builder serializer(final MessageSerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder enableSsl(final boolean enable) {
            this.enableSsl = enable;
            return this;
        }

        public Builder minInProcessPerConnection(final int minInProcessPerConnection) {
            this.minInProcessPerConnection = minInProcessPerConnection;
            return this;
        }

        public Builder maxInProcessPerConnection(final int maxInProcessPerConnection) {
            this.maxInProcessPerConnection = maxInProcessPerConnection;
            return this;
        }

        public Builder maxSimultaneousRequestsPerConnection(final int maxSimultaneousRequestsPerConnection) {
            this.maxSimultaneousRequestsPerConnection = maxSimultaneousRequestsPerConnection;
            return this;
        }

        public Builder minSimultaneousRequestsPerConnection(final int minSimultaneousRequestsPerConnection) {
            this.minSimultaneousRequestsPerConnection = minSimultaneousRequestsPerConnection;
            return this;
        }

        public Builder maxConnectionPoolSize(final int maxSize) {
            this.maxConnectionPoolSize = maxSize;
            return this;
        }

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
         * The maximum size in bytes of any request sent to the server.   This number should not exceed the same
         * setting defined on the server.
         */
        public Builder maxContentLength(final int maxContentLength) {
            this.maxContentLength = maxContentLength;
            return this;
        }

        /**
         * Time in milliseconds to wait before attempting to reconnect to a dead host after it has been marked dead.
         */
        public Builder reconnectIntialDelay(final int initialDelay) {
            this.reconnectInitialDelay = initialDelay;
            return this;
        }

        /**
         * Time in milliseconds to wait between retries when attempting to reconnect to a dead host.
         */
        public Builder reconnectInterval(final int interval) {
            this.reconnectInterval = interval;
            return this;
        }

        public Builder loadBalancingStrategy(final LoadBalancingStrategy loadBalancingStrategy) {
            this.loadBalancingStrategy = loadBalancingStrategy;
            return this;
        }

        public Builder addContactPoint(final String address) {
            try {
                this.addresses.add(InetAddress.getByName(address));
                return this;
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }

        public Builder addContactPoints(final String... addresses) {
            for (String address : addresses)
                addContactPoint(address);
            return this;
        }

        public Builder port(final int port) {
            this.port = port;
            return this;
        }

        private List<InetSocketAddress> getContactPoints() {
            return addresses.stream().map(addy -> new InetSocketAddress(addy, port)).collect(Collectors.toList());
        }

        public Cluster create() {
            if (addresses.size() == 0) addContactPoint("localhost");
            final Settings.ConnectionPoolSettings connectionPoolSettings = new Settings.ConnectionPoolSettings();
            connectionPoolSettings.maxInProcessPerConnection = this.maxInProcessPerConnection;
            connectionPoolSettings.minInProcessPerConnection = this.minInProcessPerConnection;
            connectionPoolSettings.maxSimultaneousRequestsPerConnection = this.maxSimultaneousRequestsPerConnection;
            connectionPoolSettings.minSimultaneousRequestsPerConnection = this.minSimultaneousRequestsPerConnection;
            connectionPoolSettings.maxSize = this.maxConnectionPoolSize;
            connectionPoolSettings.minSize = this.minConnectionPoolSize;
            connectionPoolSettings.maxWaitForConnection = this.maxWaitForConnection;
            connectionPoolSettings.maxContentLength = this.maxContentLength;
            connectionPoolSettings.reconnectInitialDelay = this.reconnectInitialDelay;
            connectionPoolSettings.reconnectInterval = this.reconnectInterval;
            connectionPoolSettings.resultIterationBatchSize = this.resultIterationBatchSize;
            connectionPoolSettings.enableSsl = this.enableSsl;
            return new Cluster(getContactPoints(), serializer, this.nioPoolSize, this.workerPoolSize,
                    connectionPoolSettings, loadBalancingStrategy);
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
        private ClusterInfo clusterInfo;
        private boolean initialized;
        private final List<InetSocketAddress> contactPoints;
        private final Factory factory;
        private final MessageSerializer serializer;
        private final Settings.ConnectionPoolSettings connectionPoolSettings;
        private final LoadBalancingStrategy loadBalancingStrategy;

        private final ScheduledExecutorService executor;

        private Manager(final List<InetSocketAddress> contactPoints, final MessageSerializer serializer,
                        final int nioPoolSize, final int workerPoolSize, final Settings.ConnectionPoolSettings connectionPoolSettings,
                        final LoadBalancingStrategy loadBalancingStrategy) {
            this.loadBalancingStrategy = loadBalancingStrategy;
            this.clusterInfo = new ClusterInfo(Cluster.this);
            this.contactPoints = contactPoints;
            this.connectionPoolSettings = connectionPoolSettings;
            this.factory = new Factory(nioPoolSize);
            this.serializer = serializer;
            this.executor = Executors.newScheduledThreadPool(workerPoolSize,
                    new BasicThreadFactory.Builder().namingPattern("gremlin-driver-worker-%d").build());
        }

        synchronized void init() {
            if (initialized)
                return;

            initialized = true;

            contactPoints.forEach(address -> {
                final Host host = clusterInfo.add(address);
                if (host != null)
                    host.makeAvailable();
            });
        }

        CompletableFuture<Void> close() {
            // this method is exposed publicly in both blocking and non-blocking forms.
            return CompletableFuture.supplyAsync(() -> {
                this.factory.shutdown();
                return null;
            });
        }

        @Override
        public String toString() {
            return String.join(", ", contactPoints.stream().map(InetSocketAddress::toString).collect(Collectors.<String>toList()));
        }
    }
}
