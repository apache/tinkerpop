package com.tinkerpop.gremlin.driver;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Host {
    private final InetSocketAddress address;
    private final URI webSocketUri;
    private volatile boolean isAvailable;

    public Host(final InetSocketAddress address) {
        this.address = address;
        this.webSocketUri = makeUriFromAddress(address, false);  // todo: pass down config for ssl
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public URI getWebSocketUri() {
        return webSocketUri;
    }

    void makeAvailable() {
        isAvailable = true;
    }

    void makeUnavailable() {
        isAvailable = false;
    }

    private static URI makeUriFromAddress(final InetSocketAddress addy, final boolean ssl) {
        try {
            final String scheme = ssl ? "wss" : "ws";
            return new URI(scheme, null, addy.getHostName(), addy.getPort(), "/gremlin", null, null);
        } catch (URISyntaxException use) {
            throw new RuntimeException(String.format("URI for host could not be constructed from: %s", addy), use);
        }
    }

    @Override
    public String toString() {
        return "Host{" +
                "address=" + address +
                ", webSocketUri=" + webSocketUri +
                '}';
    }
}
