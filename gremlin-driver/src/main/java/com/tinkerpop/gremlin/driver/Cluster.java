package com.tinkerpop.gremlin.driver;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A bunch of Gremlin Server instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Cluster {

    private Manager manager;

    Cluster(final List<InetSocketAddress> contactPoints) {
        this.manager = new Manager(contactPoints);
    }

    public Client connect() {
        return new Client(this);
    }

    public Client connect(final String graph) {
        return null;
    }

    public static Builder create() {
        return new Builder();
    }

    public ClusterInfo getClusterInfo() {
        return manager.clusterInfo;
    }

    public static class Builder {
        private List<InetAddress> addresses = new ArrayList<>();
        private int port = 8182;

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

    class Manager {
        private ClusterInfo clusterInfo;
        private boolean initialized;
        private final List<InetSocketAddress> contactPoints;

        private Manager(final List<InetSocketAddress> contactPoints) {
            this.clusterInfo = new ClusterInfo(this);
            this.contactPoints = contactPoints;
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

    }


}
