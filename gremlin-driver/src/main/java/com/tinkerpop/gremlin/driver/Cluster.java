package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.ser.KryoMessageSerializerV1d0;
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
import java.util.Optional;
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

    private Cluster(final List<InetSocketAddress> contactPoints, final MessageSerializer serializer) {
        this.manager = new Manager(contactPoints, serializer);
    }

    public synchronized void init() {
        if (!manager.initialized)
            manager.init();
    }

    public Client connect() {
        return new Client(this);
    }

    public Client connect(final String graph) {
        // todo: need to support this like it was in rexpro.
        return null;
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
                .port(settings.port);

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

    public static class Builder {
        private List<InetAddress> addresses = new ArrayList<>();
        private int port = 8182;
        private MessageSerializer serializer = Serializers.KRYO_V1D0.simpleInstance();

        public Builder(final String address) {
            addContactPoint(address);
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
            return new Cluster(getContactPoints(), serializer);
        }
    }

    static class Factory {
        private final EventLoopGroup group;

        public Factory() {
            final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("gremlin-driver-%d").build();
            group = new NioEventLoopGroup(4, threadFactory);
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
        private Factory factory;
        private MessageSerializer serializer;

        // todo: configurable pool size
        private  final ExecutorService executor = Executors.newCachedThreadPool();

        private Manager(final List<InetSocketAddress> contactPoints, final MessageSerializer serializer) {
            this.clusterInfo = new ClusterInfo(this);
            this.contactPoints = contactPoints;
            this.factory = new Factory();
            this.serializer = serializer;
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
    }


}
