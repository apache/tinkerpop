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

import org.apache.tinkerpop.gremlin.driver.handler.IdleConnectionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.SocketOption;
import java.net.URI;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A single connection to a Gremlin Server instance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class Connection {
    public static final int MAX_WAIT_FOR_CONNECTION = 16000;
    public static final int MAX_WAIT_FOR_CLOSE = 3000;
    public static final int RECONNECT_INTERVAL = 1000;
    public static final int RESULT_ITERATION_BATCH_SIZE = 64;
    public static final long CONNECTION_SETUP_TIMEOUT_MILLIS = 5000;
    public static final long CONNECTION_IDLE_TIMEOUT_MILLIS = 180000;
    /**
     * Default idle time in milliseconds before TCP keep-alive probes begin on an otherwise idle connection.
     */
    public static final long KEEP_ALIVE_TIME_MILLIS = 30000;
    /**
     * Default idle-read timeout in milliseconds. A value of {@code 0} disables the feature.
     */
    public static final long READ_TIMEOUT_MILLIS = 0;
    /**
     * Default maximum size in bytes of the HTTP response headers. Matches Netty's
     * {@code HttpObjectDecoder.DEFAULT_MAX_HEADER_SIZE}.
     */
    public static final int MAX_RESPONSE_HEADER_BYTES = 8192;
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    private final Channel channel;
    private final URI uri;
    private final AtomicReference<ResultSet> pending = new AtomicReference<>();
    private final Cluster cluster;
    private final Client client;
    private final ConnectionPool pool;
    private final String creatingThread;
    private final String createdTimestamp;

    /**
     * Is a {@code Connection} borrowed from the pool.
     */
    private final AtomicBoolean isBorrowed = new AtomicBoolean(false);
    /**
     * Prevents returnToPool() from being called more than once per borrow cycle.
     */
    private final AtomicBoolean returned = new AtomicBoolean(false);

    void resetReturned() {
        returned.set(false);
    }
    /**
     * This boolean guards the replace of the connection and ensures that it only occurs once.
     */
    public final AtomicBoolean isBeingReplaced = new AtomicBoolean(false);

    private final String connectionLabel;

    private final Channelizer channelizer;

    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);

    public Connection(final URI uri, final ConnectionPool pool) throws ConnectionException {
        this.uri = uri;
        this.cluster = pool.getCluster();
        this.client = pool.getClient();
        this.pool = pool;
        this.creatingThread = Thread.currentThread().getName();
        this.createdTimestamp = Instant.now().toString();
        connectionLabel = "Connection{host=" + pool.host + "}";

        if (cluster.isClosing())
            throw new IllegalStateException("Cannot open a connection with the cluster after close() is called");

        if (client.isClosing())
            throw new IllegalStateException("Cannot open a connection with the client after close() is called");

        final Bootstrap b = this.cluster.getFactory().createBootstrap();
        try {
            // Bound the TCP connection establishment (transport setup, including SSL handshake). This is the
            // connectTimeout option and applies to b.connect() below.
            configureConnectTimeout(b, cluster.getConnectTimeout());

            // Configure TCP keep-alive. SO_KEEPALIVE turns on OS-level keep-alive probes for the socket. When a
            // positive keepAliveTime is provided, the per-socket idle time before probes begin is configured via the
            // JDK extended socket option TCP_KEEPIDLE (JDK 11+, supported on Linux/macOS). That option may be
            // unavailable on some platforms/JDKs, so it is applied best-effort and is a no-op where unsupported.
            configureKeepAlive(b, cluster.getKeepAliveTime());

            channelizer = new Channelizer.HttpChannelizer();
            channelizer.init(this);
            b.channel(NioSocketChannel.class).handler(channelizer);

            channel = b.connect(uri.getHost(), uri.getPort()).sync().channel();
            channelizer.connected();

            // Configure behaviour on close of this channel. This callback would trigger the workflow to destroy this
            // connection, so that a new request doesn't pick this closed connection.
            final Connection thisConnection = this;
            channel.closeFuture().addListener((ChannelFutureListener) future -> {
                logger.debug("OnChannelClose callback called for channel {}", channel);

                // if the closeFuture is not set, it means that closeAsync() wasn't called
                if (thisConnection.closeFuture.get() == null) {
                    if (!channel.hasAttr(IdleConnectionHandler.IDLE_STATE_EVENT)) {
                        // if idle state event is not present, it means the server closed the channel for some reason.
                        // it's important to distinguish that difference in debugging
                        logger.error(String.format(
                                "Server closed the Connection on channel %s - scheduling removal from %s",
                                channel.id().asShortText(), thisConnection.pool.getPoolInfo(thisConnection)));
                    }

                    // delegate the task to scheduler thread and free up the event loop
                    thisConnection.cluster.connectionScheduler().submit(() -> thisConnection.pool.destroyConnection(thisConnection));
                }
            });
            logger.debug("Created new connection for {}", uri);
        } catch (Exception ex) {
            throw new ConnectionException(uri, "Could not open " + getConnectionInfo(true), ex);
        }
    }

    /**
     * Configures the connection establishment timeout on the supplied {@link Bootstrap}. The {@code connectTimeout}
     * is expressed in milliseconds and bounds the TCP connection setup (including the SSL handshake) performed by
     * {@code b.connect()}. It is applied via Netty's {@link ChannelOption#CONNECT_TIMEOUT_MILLIS}.
     */
    static void configureConnectTimeout(final Bootstrap b, final long connectTimeout) {
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout);
    }

    /**
     * Configures TCP keep-alive on the supplied {@link Bootstrap}. {@code SO_KEEPALIVE} is enabled whenever a
     * positive {@code keepAliveTime} is provided. The idle time before keep-alive probes begin is set via the JDK
     * extended socket option {@code TCP_KEEPIDLE} (available on JDK 11+ on Linux/macOS). When that option is not
     * available on the running platform/JDK, configuring it is skipped and {@code SO_KEEPALIVE} is still applied, so
     * the OS default idle time is used. A {@code keepAliveTime} of {@code 0} disables keep-alive entirely.
     */
    static void configureKeepAlive(final Bootstrap b, final long keepAliveTime) {
        if (keepAliveTime <= 0)
            return;

        b.option(ChannelOption.SO_KEEPALIVE, true);

        final SocketOption<Integer> tcpKeepIdle = getTcpKeepIdleOption();
        if (tcpKeepIdle != null) {
            try {
                // TCP_KEEPIDLE is expressed in seconds; resolve from milliseconds with a one second floor.
                final int keepIdleSeconds = (int) Math.max(1, keepAliveTime / 1000);
                b.option(NioChannelOption.of(tcpKeepIdle), keepIdleSeconds);
            } catch (Exception | NoClassDefFoundError ex) {
                // best-effort - leave SO_KEEPALIVE enabled with the OS default idle time
                logger.debug("Unable to configure TCP_KEEPIDLE; falling back to OS default keep-alive idle time", ex);
            }
        } else {
            logger.debug("TCP_KEEPIDLE socket option is not available on this platform/JDK; using OS default " +
                    "keep-alive idle time with SO_KEEPALIVE enabled");
        }
    }

    /**
     * Resolves the JDK {@code jdk.net.ExtendedSocketOptions.TCP_KEEPIDLE} socket option reflectively so that the
     * driver compiles and runs on platforms/JDKs where it is unavailable. Returns {@code null} when it cannot be
     * resolved.
     */
    @SuppressWarnings("unchecked")
    private static SocketOption<Integer> getTcpKeepIdleOption() {
        try {
            final Class<?> extendedSocketOptions = Class.forName("jdk.net.ExtendedSocketOptions");
            return (SocketOption<Integer>) extendedSocketOptions.getField("TCP_KEEPIDLE").get(null);
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * Consider a connection as dead if the underlying channel is not connected.
     * <p>
     * Note: A dead connection does not necessarily imply that the server is unavailable. Additional checks
     * should be performed to mark the server host as unavailable.
     */
    public boolean isDead() {
        return (channel != null && !channel.isActive());
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
        return pending;
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        if (isClosing()) return closeFuture.get();

        final CompletableFuture<Void> future = new CompletableFuture<>();
        closeFuture.set(future);

        // make sure all requests in the queue are fully processed before killing.  if they are then shutdown
        // can be immediate.  if not this method will signal the readCompleted future defined in the write()
        // operation to check if it can close.  in this way the connection no longer receives writes, but
        // can continue to read. If a request never comes back the future won't get fulfilled and the connection
        // will maintain a "pending" request, that won't quite ever go away.  The build up of such a dead requests
        // on a connection in the connection pool will force the pool to replace the connection for a fresh one.
        if (isOkToClose()) {
            if (null == channel)
                future.complete(null);
            else
                shutdown(future);
        } else {
            // there may be some pending requests. schedule a job to wait for those to complete and then shutdown
            new CheckForPending(future).runUntilDone(cluster.connectionScheduler());
        }

        return future;
    }

    public ChannelPromise write(final RequestMessage requestMessage, final CompletableFuture<ResultSet> resultSetFuture) {
        // once there is a completed write, then create a ResultSet and complete
        // the promise so that the client knows that it can start checking for results.
        final Connection thisConnection = this;

        final ChannelPromise requestPromise = channel.newPromise()
                .addListener(f -> {
                    if (!f.isSuccess()) {
                        if (logger.isDebugEnabled())
                            logger.debug(String.format("Write on connection %s failed",
                                    thisConnection.getConnectionInfo()), f.cause());

                        handleConnectionCleanupOnError(thisConnection);

                        cluster.executor().submit(() -> resultSetFuture.completeExceptionally(f.cause()));
                    } else {
                        final ResultSet resultSet = new ResultSet(cluster.executor(), requestMessage, pool.host);

                        resultSet.getReadCompleted().whenComplete((v, t) -> {
                            if (t != null) {
                                // the callback for when the read failed. a failed read means the request went to the server
                                // and came back with a server-side error of some sort.  it means the server is responsive
                                // so this isn't going to be like a potentially dead host situation which is handled above on a failed
                                // write operation.
                                logger.debug("Error while processing request on the server {}.", this, t);
                                handleConnectionCleanupOnError(thisConnection);
                            }
                            // While this request was in process, close might have been signaled in closeAsync().
                            // However, close would be blocked until all pending requests are completed. Attempt
                            // the shutdown if the returned result cleared up the last pending message and unblocked
                            // the close.
                            tryShutdown();
                        });

                        pending.set(resultSet);

                        // resultSetFuture should only be completed by a worker since the application code might have sync
                        // completion stages attached to it which and we do not want the event loop threads to process those
                        // stages.
                        cluster.executor().submit(() -> resultSetFuture.complete(resultSet));
                    }
                });
        channel.writeAndFlush(requestMessage, requestPromise);

        return requestPromise;
    }

    void returnToPool() {
        if (!returned.compareAndSet(false, true)) return;
        try {
            if (pool != null) pool.returnConnection(this);
        } catch (ConnectionException ce) {
            if (logger.isDebugEnabled())
                logger.debug("Returned {} connection to {} but an error occurred - {}", this.getConnectionInfo(), pool, ce.getMessage());
        }
    }

    private void handleConnectionCleanupOnError(final Connection thisConnection) {
        if (thisConnection.isDead() || (thisConnection.channel != null && !thisConnection.channel.isOpen())) {
            if (pool != null) pool.replaceConnection(thisConnection);
        } else {
            thisConnection.returnToPool();
        }
    }

    private boolean isOkToClose() {
        return pending.get() == null || (channel != null && !channel.isOpen()) || !pool.host.isAvailable();
    }

    /**
     * Close was signaled in closeAsync() but there were pending messages at that time. This method attempts the
     * shutdown if the returned result cleared up the last pending message.
     */
    void tryShutdown() {
        if (isClosing() && isOkToClose())
            shutdown(closeFuture.get());
    }

    private void shutdown(final CompletableFuture<Void> future) {
        // shutdown can be called directly from closeAsync() or after write() and therefore this method should only
        // be called once. once shutdown is initiated, it shouldn't be executed a second time or else it sends more
        // messages at the server and leads to ugly log messages over there.
        //
        // this method used to be synchronized prior to 3.4.11, but it seems to be able to leave the driver in a
        // hanging state because of the write() down below that can call back in on shutdown() (which is weird i
        // guess). that seems to put the executor thread in a monitor state that it doesn't recover from. since all
        // the code in here is behind shutdownInitiated the synchronized doesn't seem necessary
        if (shutdownInitiated.compareAndSet(false, true)) {
            // take a defensive posture here in the event the channelizer didn't get initialized somehow and a
            // close() on the Connection is still called
            if (channelizer != null)
                channelizer.close(channel);

            // seems possible that the channelizer could initialize but fail to produce a channel, so worth checking
            // null before proceeding here. also if the cluster is in shutdown then the event loop could be shutdown
            // already and there will be no way to get a new promise out there.
            if (channel != null) {
                final ChannelPromise promise = channel.newPromise();
                promise.addListener(f -> {
                    if (f.cause() != null) {
                        future.completeExceptionally(f.cause());
                    } else {
                        if (logger.isDebugEnabled())
                            logger.debug("{} destroyed successfully.", this.getConnectionInfo());

                        future.complete(null);
                    }
                });

                // close the netty channel, if not already closed
                if (!channel.closeFuture().isDone()) {
                    channel.close(promise);
                } else {
                    if (!promise.trySuccess()) {
                        logger.warn("Failed to mark a promise as success because it is done already: {}", promise);
                    }
                }
            } else {
                // if we dont handle the supplied future it can hang the close
                future.complete(null);
            }
        } else {
            // if we dont handle the supplied future it can hang the close
            future.complete(null);
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
     *
     * @param showHost determines if the {@link Host} should be displayed in the message.
     */
    public String getConnectionInfo(final boolean showHost) {
        return showHost ?
                String.format("Connection{channel=%s host=%s isDead=%s borrowed=%s pending=%s markedReplaced=%s closing=%s created=%s thread=%s}",
                        getChannelId(), pool.host.toString(), isDead(), this.isBorrowed().get(), getPending().get() == null ? 0 : 1, this.isBeingReplaced, isClosing(), createdTimestamp, creatingThread) :
                String.format("Connection{channel=%s isDead=%s borrowed=%s pending=%s markedReplaced=%s closing=%s created=%s thread=%s}",
                        getChannelId(), isDead(), this.isBorrowed().get(), getPending().get() == null ? 0 : 1, this.isBeingReplaced, isClosing(), createdTimestamp, creatingThread);
    }

    /**
     * Returns the short ID for the underlying channel for this connection.
     * <p>
     * Visible for testing.
     */
    String getChannelId() {
        return (channel != null) ? channel.id().asShortText() : "null";
    }

    @Override
    public String toString() {
        return String.format(connectionLabel + ", {channel=%s}", getChannelId());
    }

    /**
     * Self-cancelling tasks that periodically checks for the pending queue to clear before shutting down the
     * {@code Connection}. Once it does that, it self cancels the scheduled job in the executor.
     */
    private final class CheckForPending implements Runnable {
        private volatile ScheduledFuture<?> self;
        private final CompletableFuture<Void> future;
        private long checkUntil = System.currentTimeMillis();

        CheckForPending(final CompletableFuture<Void> future) {
            this.future = future;
            checkUntil = checkUntil + cluster.getMaxWaitForClose();
        }

        @Override
        public void run() {
            logger.info("Checking for pending messages to complete before close on {}", this);

            if (isOkToClose() || System.currentTimeMillis() > checkUntil) {
                shutdown(future);
                boolean interrupted = false;
                try {
                    while (null == self) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            interrupted = true;
                        }
                    }
                    self.cancel(false);
                } finally {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        void runUntilDone(final ScheduledExecutorService executor) {
            self = executor.scheduleAtFixedRate(this, 1000, 1000, TimeUnit.MILLISECONDS);
        }
    }
}
