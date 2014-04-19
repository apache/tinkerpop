package com.tinkerpop.gremlin.driver;

import java.net.InetSocketAddress;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Host {
    private final InetSocketAddress address;
    private volatile boolean isAvailable;

    public Host(final InetSocketAddress address) {
        this.address = address;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    void makeAvailable() {
        isAvailable = true;
    }

    void makeUnavailable() {
        isAvailable = false;
    }
}
