/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A single connection to a Gremlin Server instance. Now a thin wrapper around {@link HttpTransport}
 * that maintains compatibility with {@link ConnectionPool}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class Connection {
    public static final int MAX_WAIT_FOR_CONNECTION = 16000;
    public static final int MAX_WAIT_FOR_CLOSE = 3000;
    public static final long MAX_RESPONSE_CONTENT_LENGTH = Integer.MAX_VALUE;
    public static final int RECONNECT_INTERVAL = 1000;
    public static final int RESULT_ITERATION_BATCH_SIZE = 64;
    public static final long CONNECTION_SETUP_TIMEOUT_MILLIS = 15000;
    public static final long CONNECTION_IDLE_TIMEOUT_MILLIS = 180000;
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    private final URI uri;
    private final Cluster cluster;
    private final ConnectionPool pool;
    private final String creatingThread;
    private final String createdTimestamp;

    /**
     * Is a {@code Connection} borrowed from the pool.
     */
    private final AtomicBoolean isBorrowed = new AtomicBoolean(false);
    /**
     * This boolean guards the replace of the connection and ensures that it only occurs once.
     */
    public final AtomicBoolean isBeingReplaced = new AtomicBoolean(false);

    private final String connectionLabel;
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();
    private final AtomicBoolean dead = new AtomicBoolean(false);

    public Connection(final URI uri, final ConnectionPool pool) throws ConnectionException {
        this.uri = uri;
        this.cluster = pool.getCluster();
        this.pool = pool;
        this.creatingThread = Thread.currentThread().getName();
        this.createdTimestamp = Instant.now().toString();
        connectionLabel = "Connection{host=" + pool.host + "}";

        if (cluster.isClosing())
            throw new IllegalStateException("Cannot open a connection with the cluster after close() is called");

        // Verify the host is reachable by attempting a TCP connection
        final int port = uri.getPort() > 0 ? uri.getPort() : (uri.getScheme().equals("https") ? 443 : 80);
        final long timeout = cluster.connectionPoolSettings().connectionSetupTimeoutMillis;
        try (final Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(uri.getHost(), port), (int) Math.min(timeout, Integer.MAX_VALUE));
        } catch (final IOException e) {
            throw new ConnectionException(uri, pool.host.getAddress(),
                    "Could not connect to " + uri + " - " + e.getMessage(), e);
        }

        logger.debug("Created new connection for {}", uri);
    }

    /**
     * Consider a connection as dead if it has been explicitly marked dead.
     */
    public boolean isDead() {
        return dead.get();
    }

    void markDead() {
        dead.set(true);
    }

    public AtomicBoolean isBorrowed() {
        return isBorrowed;
    }

    boolean isClosing() {
        return closeFuture.get() != null;
    }

    URI getUri() {
        return uri;
    }

    Cluster getCluster() {
        return cluster;
    }

    AtomicReference<ResultSet> getPending() {
        return new AtomicReference<>();
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        if (isClosing()) return closeFuture.get();

        final CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        closeFuture.set(future);
        return future;
    }

    public void write(final RequestMessage requestMessage, final CompletableFuture<ResultSet> resultSetFuture) {
        try {
            cluster.getHttpTransport().sendAsync(pool.host, requestMessage)
                    .whenComplete((resultSet, throwable) -> {
                        if (throwable != null) {
                            markDead();
                            resultSetFuture.completeExceptionally(throwable);
                        } else {
                            resultSetFuture.complete(resultSet);
                        }
                        returnToPool();
                    });
        } catch (final Exception ex) {
            resultSetFuture.completeExceptionally(ex);
            returnToPool();
        }
    }

    private void returnToPool() {
        try {
            pool.returnConnection(this);
        } catch (final Exception e) {
            logger.debug("Error returning connection to pool", e);
        }
    }

    /**
     * Gets a message that describes the state of the connection.
     */
    public String getConnectionInfo() {
        return this.getConnectionInfo(true);
    }

    /**
     * Gets a message that describes the state of the connection.
     */
    public String getConnectionInfo(final boolean showHost) {
        return showHost ?
                String.format("Connection{host=%s isDead=%s borrowed=%s markedReplaced=%s closing=%s created=%s thread=%s}",
                        pool.host.toString(), isDead(), this.isBorrowed().get(), this.isBeingReplaced, isClosing(), createdTimestamp, creatingThread) :
                String.format("Connection{isDead=%s borrowed=%s markedReplaced=%s closing=%s created=%s thread=%s}",
                        isDead(), this.isBorrowed().get(), this.isBeingReplaced, isClosing(), createdTimestamp, creatingThread);
    }

    @Override
    public String toString() {
        return connectionLabel;
    }
}
