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
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A single connection to a Gremlin Server instance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class Connection {
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    private final Channel channel;
    private final URI uri;
    private final ConcurrentMap<UUID, ResultQueue> pending = new ConcurrentHashMap<>();
    private final Cluster cluster;
    private final Client client;
    private final ConnectionPool pool;
    private final long keepAliveInterval;

    public static final int MAX_IN_PROCESS = 4;
    public static final int MIN_IN_PROCESS = 1;
    public static final int MAX_WAIT_FOR_CONNECTION = 16000;
    public static final int MAX_WAIT_FOR_SESSION_CLOSE = 3000;
    public static final int MAX_WAIT_FOR_CLOSE = 3000;
    public static final int MAX_CONTENT_LENGTH = 65536;

    public static final int RECONNECT_INTERVAL = 1000;
    public static final int RESULT_ITERATION_BATCH_SIZE = 64;
    public static final long KEEP_ALIVE_INTERVAL = 180000;

    /**
     * When a {@code Connection} is borrowed from the pool, this number is incremented to indicate the number of
     * times it has been taken and is decremented when it is returned.  This number is one indication as to how
     * busy a particular {@code Connection} is.
     */
    public final AtomicInteger borrowed = new AtomicInteger(0);
    /**
     * This boolean guards the replace of the connection and ensures that it only occurs once.
     */
    public final AtomicBoolean isBeingReplaced = new AtomicBoolean(false);
    private final AtomicReference<Class<Channelizer>> channelizerClass = new AtomicReference<>(null);

    private final int maxInProcess;

    private final String connectionLabel;

    private final Channelizer channelizer;

    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
    private final AtomicReference<ScheduledFuture> keepAliveFuture = new AtomicReference<>();

    public Connection(final URI uri, final ConnectionPool pool, final int maxInProcess) throws ConnectionException {
        this.uri = uri;
        this.cluster = pool.getCluster();
        this.client = pool.getClient();
        this.pool = pool;
        this.maxInProcess = maxInProcess;
        this.keepAliveInterval = pool.settings().keepAliveInterval;

        connectionLabel = String.format("Connection{host=%s}", pool.host);

        if (cluster.isClosing())
            throw new IllegalStateException("Cannot open a connection with the cluster after close() is called");

        final Bootstrap b = this.cluster.getFactory().createBootstrap();
        try {
            if (channelizerClass.get() == null) {
                channelizerClass.compareAndSet(null, (Class<Channelizer>) Class.forName(cluster.connectionPoolSettings().channelizer));
            }

            channelizer = channelizerClass.get().newInstance();
            channelizer.init(this);
            b.channel(NioSocketChannel.class).handler(channelizer);

            channel = b.connect(uri.getHost(), uri.getPort()).sync().channel();
            channelizer.connected();

            /* Configure behaviour on close of this channel.
             *
             * This callback would trigger the workflow to replace this connection. ReplaceConnection workflow might be
             * called twice, once from this workflow and once again from actions taken by channelInactive callback.
             * In such scenarios, isBeingReplaced boolean is used to ensure that the connection is only replaced once.
             */
            final Connection thisConnection = this;
            channel.closeFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    logger.debug("OnChannelClose future called for channel {}", thisConnection.getChannelId());
                    // Replace the channel if it was not intentionally closed using CloseAsync method.
                    if (closeFuture.get() == null) {
                        // delegate the task to worker thread and free up the event loop
                        cluster.executor().submit(() -> pool.replaceConnection(thisConnection));
                    }
                }
            });

            logger.info("Created new connection for {}", uri);

            scheduleKeepAlive();
        } catch (Exception ie) {
            logger.debug("Error opening connection on {}", uri);
            throw new ConnectionException(uri, "Could not open connection", ie);
        }
    }

    /**
     * A connection can only have so many things in process happening on it at once, where "in process" refers to
     * the maximum number of in-process requests less the number of pending responses.
     */
    public int availableInProcess() {
        // no need for a negative available amount - not sure that the pending size can ever exceed maximum, but
        // better to avoid the negatives that would ensue if it did
        return Math.max(0, maxInProcess - pending.size());
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

    boolean isClosing() {
        return closeFuture.get() != null;
    }

    URI getUri() {
        return uri;
    }

    Cluster getCluster() {
        return cluster;
    }

    Client getClient() {
        return client;
    }

    ConcurrentMap<UUID, ResultQueue> getPending() {
        return pending;
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        if (isClosing()) return closeFuture.get();

        final CompletableFuture<Void> future = new CompletableFuture<>();
        closeFuture.set(future);

        // stop any pings being sent at the server for keep-alive
        final ScheduledFuture keepAlive = keepAliveFuture.get();
        if (keepAlive != null) keepAlive.cancel(true);

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
            new CheckForPending(future).runUntilDone(cluster.executor());
        }

        return future;
    }

    public ChannelPromise write(final RequestMessage requestMessage, final CompletableFuture<ResultSet> future) {
        // once there is a completed write, then create a traverser for the result set and complete
        // the promise so that the client knows that that it can start checking for results.
        final Connection thisConnection = this;

        final ChannelPromise requestPromise = channel.newPromise()
                .addListener(f -> {
                    if (!f.isSuccess()) {
                        if (logger.isDebugEnabled())
                            logger.debug(String.format("Write on connection %s failed", thisConnection.getConnectionInfo()), f.cause());

                        handleConnectionCleanupOnError(thisConnection);

                        cluster.executor().submit(() -> future.completeExceptionally(f.cause()));
                    } else {
                        final LinkedBlockingQueue<Result> resultLinkedBlockingQueue = new LinkedBlockingQueue<>();
                        final CompletableFuture<Void> readCompleted = new CompletableFuture<>();

                        // the callback for when the read was successful, meaning that ResultQueue.markComplete()
                        // was called
                        readCompleted.thenAcceptAsync(v -> {
                            // connection is fine, just return it to the pool
                            thisConnection.returnToPool();

                            // While this request was in process, close might have been signaled in closeAsync().
                            // However, close would be blocked until all pending requests are completed. Attempt
                            // the shutdown if the returned result cleared up the last pending message and unblocked
                            // the close.
                            tryShutdown();
                        }, cluster.executor());

                        // the callback for when the read failed. a failed read means the request went to the server
                        // and came back with a server-side error of some sort.  it means the server is responsive
                        // so this isn't going to be like a potentially dead host situation which is handled above on a failed
                        // write operation.
                        readCompleted.exceptionally(t -> {

                            handleConnectionCleanupOnError(thisConnection);

                            // While this request was in process, close might have been signaled in closeAsync().
                            // However, close would be blocked until all pending requests are completed. Attempt
                            // the shutdown if the returned result cleared up the last pending message and unblocked
                            // the close.
                            tryShutdown();

                            return null;
                        });

                        final ResultQueue handler = new ResultQueue(resultLinkedBlockingQueue, readCompleted);
                        pending.put(requestMessage.getRequestId(), handler);
                        cluster.executor().submit(() -> future.complete(
                                new ResultSet(handler, cluster.executor(), readCompleted, requestMessage, pool.host)));
                    }
                });
        channel.writeAndFlush(requestMessage, requestPromise);

        scheduleKeepAlive();

        return requestPromise;
    }

    private void scheduleKeepAlive() {
        final Connection thisConnection = this;
        // try to keep the connection alive if the channel allows such things - websockets will
        if (channelizer.supportsKeepAlive() && keepAliveInterval > 0) {

            final ScheduledFuture oldKeepAliveFuture = keepAliveFuture.getAndSet(cluster.executor().scheduleAtFixedRate(() -> {
                logger.debug("Request sent to server to keep {} alive", thisConnection);
                try {
                    channel.writeAndFlush(channelizer.createKeepAliveMessage());
                } catch (Exception ex) {
                    // will just log this for now - a future real request can be responsible for the failure that
                    // marks the host as dead. this also may not mean the host is actually dead. more robust handling
                    // is in play for real requests, not this simple ping
                    logger.warn(String.format("Keep-alive did not succeed on %s", thisConnection), ex);
                }
            }, keepAliveInterval, keepAliveInterval, TimeUnit.MILLISECONDS));

            // try to cancel the old future if it's still un-executed - no need to ping since a new write has come
            // through on the connection
            if (oldKeepAliveFuture != null) oldKeepAliveFuture.cancel(true);
        }
    }

    private void returnToPool() {
        try {
            if (pool != null) pool.returnConnection(this);
        } catch (ConnectionException ce) {
            if (logger.isDebugEnabled())
                logger.debug("Returned {} connection to {} but an error occurred - {}", this.getConnectionInfo(), pool, ce.getMessage());
        }
    }

    private void handleConnectionCleanupOnError(final Connection thisConnection) {
        if (thisConnection.isDead()) {
            if (pool != null) pool.replaceConnection(thisConnection);
        } else {
            thisConnection.returnToPool();
        }
    }

    private boolean isOkToClose() {
        return pending.isEmpty() || (channel != null && !channel.isOpen()) || !pool.host.isAvailable();
    }

    /**
     * Close was signaled in closeAsync() but there were pending messages at that time. This method attempts the
     * shutdown if the returned result cleared up the last pending message.
     */
    private void tryShutdown() {
        if (isClosing() && isOkToClose())
            shutdown(closeFuture.get());
    }

    private synchronized void shutdown(final CompletableFuture<Void> future) {
        // shutdown can be called directly from closeAsync() or after write() and therefore this method should only
        // be called once. once shutdown is initiated, it shouldn't be executed a second time or else it sends more
        // messages at the server and leads to ugly log messages over there.
        if (shutdownInitiated.compareAndSet(false, true)) {
            final String connectionInfo = this.getConnectionInfo();
            // this block of code that "closes" the session is deprecated as of 3.3.11 - this message is going to be
            // removed at 3.5.0. we will instead bind session closing to the close of the channel itself and not have
            // this secondary operation here which really only acts as a means for clearing resources in a functioning
            // session. "functioning" in this context means that the session is not locked up with a long running
            // operation which will delay this close execution which ideally should be more immediate, as in the user
            // is annoyed that a long run operation is happening and they want an immediate cancellation. that's the
            // most likely use case. we also get the nice benefit that this if/then code just goes away as the
            // Connection really shouldn't care about the specific Client implementation.
            if (client instanceof Client.SessionedClient) {
                final boolean forceClose = client.getSettings().getSession().get().isForceClosed();
                final RequestMessage closeMessage = client.buildMessage(
                        RequestMessage.build(Tokens.OPS_CLOSE).addArg(Tokens.ARGS_FORCE, forceClose)).create();

                final CompletableFuture<ResultSet> closed = new CompletableFuture<>();
                write(closeMessage, closed);

                try {
                    // make sure we get a response here to validate that things closed as expected.  on error, we'll let
                    // the server try to clean up on its own.  the primary error here should probably be related to
                    // protocol issues which should not be something a user has to fuss with.
                    closed.join().all().get(cluster.getMaxWaitForSessionClose(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException ex) {
                    final String msg = String.format(
                            "Timeout while trying to close connection on %s - force closing - server will close session on shutdown or expiration.",
                            ((Client.SessionedClient) client).getSessionId());
                    logger.warn(msg, ex);
                } catch (Exception ex) {
                    final String msg = String.format(
                            "Encountered an error trying to close connection on %s - force closing - server will close session on shutdown or expiration.",
                            ((Client.SessionedClient) client).getSessionId());
                    logger.warn(msg, ex);
                }
            }

            channelizer.close(channel);

            final ChannelPromise promise = channel.newPromise();
            promise.addListener(f -> {
                if (f.cause() != null) {
                    future.completeExceptionally(f.cause());
                } else {
                    if (logger.isDebugEnabled())
                        logger.debug("{} destroyed successfully.", connectionInfo);

                    future.complete(null);
                }
            });

            // close the netty channel, if not already closed
            if (!channel.closeFuture().isDone()) {
                channel.close(promise);
            } else {
                if (!(promise instanceof io.netty.channel.VoidChannelPromise) && !promise.trySuccess()) {
                    logger.warn("Failed to mark a promise as success because it is done already: {}", promise);
                }
            }
        }
    }

    public String getConnectionInfo() {
        return String.format("Connection{host=%s, isDead=%s, borrowed=%s, pending=%s}",
                pool.host, isDead(), borrowed, pending.size());
    }

    /**
     * Returns the short ID for the underlying channel for this connection.
     * <p>
     * Currently only used for testing.
     */
    String getChannelId() {
        return channel.id().asShortText();
    }

    @Override
    public String toString() {
        return connectionLabel;
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
