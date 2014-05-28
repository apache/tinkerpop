package com.tinkerpop.gremlin.driver;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ClusterInfo {

    private final Cluster cluster;
    private final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<>();

    public ClusterInfo(final Cluster cluster) {
        this.cluster = cluster;
    }

    public Host add(final InetSocketAddress address) {
        final Host newHost = new Host(address, cluster);
        final Host previous = hosts.putIfAbsent(address, newHost);
        return previous == null ? newHost : null;
    }

    Collection<Host> allHosts() {
        return hosts.values();
    }
}
