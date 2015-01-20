package com.tinkerpop.gremlin.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Host {
    private static final Logger logger = LoggerFactory.getLogger(Host.class);
    private final InetSocketAddress address;
    private final URI hostUri;
    private volatile boolean isAvailable;
    private final Cluster cluster;

    final AtomicReference<ScheduledFuture<?>> reconnectionAttempt = new AtomicReference<>(null);

    public Host(final InetSocketAddress address, final Cluster cluster) {
        this.cluster = cluster;
        this.address = address;
        this.hostUri = makeUriFromAddress(address, cluster.connectionPoolSettings().enableSsl);
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public URI getHostUri() {
        return hostUri;
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
                        }, cluster.connectionPoolSettings().reconnectInitialDelay,
                        cluster.connectionPoolSettings().reconnectInterval, TimeUnit.MILLISECONDS));
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
                ", hostUri=" + hostUri +
                '}';
    }

    public static interface Listener {
        public void onAvailable(final Host host);

        public void onUnavailable(final Host host);

        public void onNew(final Host host);

        public void onRemove(final Host host);
    }
}
