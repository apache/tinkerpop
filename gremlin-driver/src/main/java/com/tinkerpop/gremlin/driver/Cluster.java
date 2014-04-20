package com.tinkerpop.gremlin.driver;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A bunch of Gremlin Server instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Cluster {

    private Manager manager;

    private Cluster(final List<InetSocketAddress> contactPoints) {
        this.manager = new Manager(contactPoints);
    }

    public synchronized void init() {
        if (!manager.initialized)
            manager.init();
    }

    public Client connect() {
        return new Client(this);
    }

    public Client connect(final String graph) {
        return null;
    }

    public static Builder create(final String address) {
        return new Builder(address);
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
        return manager.getFactory();
    }

    public static class Builder {
        private List<InetAddress> addresses = new ArrayList<>();
        private int port = 8182;

        public Builder(final String address) {
            addContactPoint(address);
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
            return new Cluster(getContactPoints());
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

        private Manager(final List<InetSocketAddress> contactPoints) {
            this.clusterInfo = new ClusterInfo(this);
            this.contactPoints = contactPoints;
            this.factory = new Factory();
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

        Factory getFactory() {
            return factory;
        }

        CompletableFuture<Void> close() {
            return CompletableFuture.supplyAsync(() -> {
                this.factory.shutdown();
                return null;
            });
        }
    }


}
