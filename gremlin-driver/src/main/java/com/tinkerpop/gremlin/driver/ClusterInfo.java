package com.tinkerpop.gremlin.driver;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ClusterInfo {

    private final Cluster.Manager manager;
    private final ConcurrentMap<InetSocketAddress, Host> hosts = new ConcurrentHashMap<>();

    public ClusterInfo(final Cluster.Manager manager) {
        this.manager = manager;
    }

    public Host add(final InetSocketAddress address) {
        final Host newHost = new Host(address);
        final Host previous = hosts.putIfAbsent(address, newHost);
        return previous == null ? newHost : null;
    }

    Collection<Host> allHosts() {
        return hosts.values();
    }
}
