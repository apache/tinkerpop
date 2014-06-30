package com.tinkerpop.gremlin.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Host {
    private static final Logger logger = LoggerFactory.getLogger(Host.class);
    private final InetSocketAddress address;
    private final URI webSocketUri;
    private volatile boolean isAvailable;
    private final Cluster cluster;

    final AtomicReference<ScheduledFuture<?>> reconnectionAttempt = new AtomicReference<>(null);

    public Host(final InetSocketAddress address, final Cluster cluster) {
        this.cluster = cluster;
        this.address = address;
        this.webSocketUri = makeUriFromAddress(address, false);  // todo: pass down config for ssl
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public URI getWebSocketUri() {
        return webSocketUri;
    }

    public boolean isAvailable() {
        return isAvailable;
    }

    void makeAvailable() {
        isAvailable = true;
    }

    void makeUnavailable(final Function<Host, Boolean> reconnect) {
        isAvailable = false;

        // only do a connection re-attempt if one is not already in progress
        reconnectionAttempt.compareAndSet(null,
                this.cluster.executor().scheduleAtFixedRate(() -> {
                    logger.debug("Trying to reconnect to dead host at {}", this);
                    if (reconnect.apply(this)) reconnected();
                }, 1000, 1000, TimeUnit.MILLISECONDS));  // todo: configuration
    }

    private void reconnected() {
        reconnectionAttempt.get().cancel(false);
        reconnectionAttempt.set(null);
        makeAvailable();
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

    public static interface Listener {
        public void onAvailable(final Host host);

        public void onUnavailable(final Host host);

        public void onNew(final Host host);

        public void onRemove(final Host host);
    }
}
