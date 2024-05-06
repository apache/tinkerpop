/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

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
 * Identifies a server within the {@link Cluster} at a specific address.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Host {
    private static final Logger logger = LoggerFactory.getLogger(Host.class);
    private final InetSocketAddress address;
    private final URI hostUri;
    private volatile boolean isAvailable;
    private final Cluster cluster;
    private final String hostLabel;

    final AtomicReference<Boolean> retryInProgress = new AtomicReference<>(Boolean.FALSE);
    ScheduledFuture<?> retryThread = null;

    Host(final InetSocketAddress address, final Cluster cluster) {
        this.cluster = cluster;
        this.address = address;
        this.hostUri = makeUriFromAddress(address, cluster.getPath(), cluster.connectionPoolSettings().enableSsl);
        hostLabel = String.format("Host{address=%s, hostUri=%s}", address, hostUri);
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

        if (isAvailable)
            logger.warn("Marking {} as unavailable. Trying to reconnect.", this);

        isAvailable = false;

        // only do a connection re-attempt if one is not already in progress
        if (retryInProgress.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
            retryThread = this.cluster.hostScheduler().scheduleAtFixedRate(() -> {
                    logger.debug("Trying to reconnect to dead host at {}", this);
                    if (reconnect.apply(this)) reconnected();
                }, cluster.connectionPoolSettings().reconnectInterval,
                cluster.connectionPoolSettings().reconnectInterval, TimeUnit.MILLISECONDS);
        }
    }

    void tryReconnectingImmediately(final Function<Host, Boolean> reconnect) {
        // only do a connection re-attempt if one is not already in progress
        if (retryInProgress.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
            retryThread = this.cluster.hostScheduler().scheduleAtFixedRate(() -> {
                        logger.debug("Trying to reconnect to host at {}", this);
                        final boolean reconnected = reconnect.apply(this);
                        if (reconnected)
                            reconnected();
                        else {
                            logger.warn("Marking {} as unavailable. Trying to reconnect.", this);
                            isAvailable = false;
                        }
                    }, 0,
                    cluster.connectionPoolSettings().reconnectInterval, TimeUnit.MILLISECONDS);
        }
    }

    private void reconnected() {
        // race condition!  retry boolean could be set to false, a new retryThread created above
        // and then cancelled here.   But we're only executing this at all because we *have* reconnected
        retryThread.cancel(false);
        retryThread = null;
        retryInProgress.set(Boolean.FALSE);
        makeAvailable();
    }

    private static URI makeUriFromAddress(final InetSocketAddress addy, final String path, final boolean ssl) {
        final Channelizer channelizer = new Channelizer.HttpChannelizer();

        try {
            final String scheme = channelizer.getScheme(ssl);
            return new URI(scheme, null, addy.getHostName(), addy.getPort(), path, null, null);
        } catch (URISyntaxException use) {
            throw new RuntimeException(String.format("URI for host could not be constructed from: %s", addy), use);
        }
    }

    @Override
    public String toString() {
        return hostLabel;
    }

    public interface Listener {
        void onAvailable(final Host host);

        void onUnavailable(final Host host);

        void onNew(final Host host);

        void onRemove(final Host host);
    }
}
