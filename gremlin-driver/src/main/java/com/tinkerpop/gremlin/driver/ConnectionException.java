package com.tinkerpop.gremlin.driver;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ConnectionException extends Exception {
    private Host host;
    public ConnectionException(final Host host, final String message) {
        super(message);
        this.host = host;
    }

    public ConnectionException(final Host host, final String message, final Throwable cause) {
        super(message, cause);
        this.host = host;
    }

    public URI getUri() {
        return host.getWebSocketUri();
    }

    public InetSocketAddress getAddress() {
        return host.getAddress();
    }
}
