package com.tinkerpop.gremlin.driver.exception;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ConnectionException extends Exception {
    private URI uri;
    private InetSocketAddress address;

    public ConnectionException(final URI uri, final InetSocketAddress addy, final String message) {
        super(message);
        this.address = addy;
        this.uri = uri;
    }

    public ConnectionException(final URI uri, final InetSocketAddress addy, final String message, final Throwable cause) {
        super(message, cause);
        this.address = addy;
        this.uri = uri;
    }

    public URI getUri() {
        return uri;
    }

    public InetSocketAddress getAddress() {
        return address;
    }
}
