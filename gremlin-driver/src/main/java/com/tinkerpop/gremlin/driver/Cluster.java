package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.ser.Serializers;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * A bunch of Gremlin Server instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Cluster {

    private Manager manager;

    private Cluster(final List<InetSocketAddress> contactPoints, final MessageSerializer serializer,
                    final int nioPoolSize, final int workerPoolSize, final Settings.ConnectionPoolSettings connectionPoolSettings) {
        this.manager = new Manager(contactPoints, serializer, nioPoolSize, workerPoolSize, connectionPoolSettings);
    }

    public synchronized void init() {
        if (!manager.initialized)
            manager.init();
    }

    public Client connect() {
        return new Client(this);
    }

    @Override
    public String toString() {
        return manager.toString();
    }

    public static Builder create() {
        return new Builder();
    }

    public static Builder create(final String address) {
        return new Builder(address);
    }

    public static Builder create(final File configurationFile) throws FileNotFoundException {
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

        builder.serializer(settings.serializer);

        return builder;
    }

    /**
     * Create a {@code Cluster} with all default settings which will connect to one contact point at {@code localhost}.
     */
    public static Cluster open() {
        return create("localhost").build();
    }

    /**
     * Create a {@code Cluster} using a YAML-based configuration file.
     */
    public static Cluster open(final String configurationFile) throws Exception {
        final File file = new File(configurationFile);
        if (!file.exists())
            throw new IllegalArgumentException(String.format("Configuration file at %s does not exist", configurationFile));

        return create(file).build();
    }

    public ClusterInfo getClusterInfo() {
        return manager.clusterInfo;
    }

    public void close() {
        closeAsync().join();
    }

    public CompletableFuture<Void> closeAsync() {
        return manager.close();
    }

    Factory getFactory() {
        return manager.factory;
    }

    MessageSerializer getSerializer() {
        return manager.serializer;
    }

    ExecutorService executor() {
        return manager.executor;
    }

    Settings.ConnectionPoolSettings connectionPoolSettings() {
        return manager.connectionPoolSettings;
    }

    public static class Builder {
        private List<InetAddress> addresses = new ArrayList<>();
        private int port = 8182;
        private MessageSerializer serializer = Serializers.KRYO_V1D0.simpleInstance();
        private int nioPoolSize = Runtime.getRuntime().availableProcessors();
        private int workerPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        private int minConnectionPoolSize = ConnectionPool.MIN_POOL_SIZE;
        private int maxConnectionPoolSize = ConnectionPool.MAX_POOL_SIZE;
        private int minSimultaneousRequestsPerConnection = ConnectionPool.MIN_SIMULTANEOUS_REQUESTS_PER_CONNECTION;
        private int maxSimultaneousRequestsPerConnection = ConnectionPool.MAX_SIMULTANEOUS_REQUESTS_PER_CONNECTION;
        private int maxInProcessPerConnection = Connection.MAX_IN_PROCESS;
        private int minInProcessPerConnection = Connection.MIN_IN_PROCESS;

        private Builder() {
            // empty to prevent direct instantiation
        }

        private Builder(final String address) {
            addContactPoint(address);
        }

        /**
         * Size of the pool for handling request/response operations.  Defaults to the number of available processors.
         */
        public Builder nioPoolSize(int nioPoolSize) {
            if (nioPoolSize < 1) throw new IllegalArgumentException("The workerPoolSize must be greater than zero");
            this.nioPoolSize = nioPoolSize;
            return this;
        }

        /**
         * Size of the pool for handling background work.  Defaults to the number of available processors multiplied
         * by 2
         */
        public Builder workerPoolSize(int workerPoolSize) {
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

        public Cluster build() {
            if (addresses.size() == 0) addContactPoint("localhost");
            final Settings.ConnectionPoolSettings connectionPoolSettings = new Settings.ConnectionPoolSettings();
            connectionPoolSettings.maxInProcessPerConnection = this.maxInProcessPerConnection;
            connectionPoolSettings.minInProcessPerConnection = this.minInProcessPerConnection;
            connectionPoolSettings.maxSimultaneousRequestsPerConnection = this.maxSimultaneousRequestsPerConnection;
            connectionPoolSettings.minSimultaneousRequestsPerConnection = this.minSimultaneousRequestsPerConnection;
            connectionPoolSettings.maxSize = this.maxConnectionPoolSize;
            connectionPoolSettings.minSize = this.minConnectionPoolSize;
            return new Cluster(getContactPoints(), serializer, this.nioPoolSize, this.workerPoolSize, connectionPoolSettings);
        }
    }

    static class Factory {
        private final EventLoopGroup group;

        public Factory(final int nioPoolSize) {
            final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("gremlin-driver-nio-%d").build();
            group = new NioEventLoopGroup(nioPoolSize, threadFactory);
        }

        Bootstrap createBootstrap() {
            return new Bootstrap().group(group);
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

        private final ExecutorService executor;

        private Manager(final List<InetSocketAddress> contactPoints, final MessageSerializer serializer,
                        final int nioPoolSize, final int workerPoolSize, final Settings.ConnectionPoolSettings connectionPoolSettings) {
            this.clusterInfo = new ClusterInfo(this);
            this.contactPoints = contactPoints;
            this.connectionPoolSettings = connectionPoolSettings;
            this.factory = new Factory(nioPoolSize);
            this.serializer = serializer;
            this.executor =  Executors.newFixedThreadPool(workerPoolSize,
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
