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

import org.apache.tinkerpop.gremlin.util.Tokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.URI;
import java.time.Instant;
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
    private final String creatingThread;
    private final String createdTimestamp;

    public static final int MAX_IN_PROCESS = 1;
    public static final int MIN_IN_PROCESS = 0;
    public static final int MAX_WAIT_FOR_CONNECTION = 16000;
    public static final int MAX_WAIT_FOR_CLOSE = 3000;
    public static final int MAX_CONTENT_LENGTH = 10 * 1024 * 1024;

    public static final int RECONNECT_INTERVAL = 1000;
    public static final int RESULT_ITERATION_BATCH_SIZE = 64;
    public static final long KEEP_ALIVE_INTERVAL = 180000;
    public final static long CONNECTION_SETUP_TIMEOUT_MILLIS = 15000;

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

    public Connection(final URI uri, final ConnectionPool pool, final int maxInProcess) throws ConnectionException {
        this.uri = uri;
        this.cluster = pool.getCluster();
        this.client = pool.getClient();
        this.pool = pool;
        this.maxInProcess = maxInProcess;
        this.creatingThread = Thread.currentThread().getName();
        this.createdTimestamp = Instant.now().toString();
        connectionLabel = "Connection{host=" + pool.host + "}";

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

            // Configure behaviour on close of this channel. This callback would trigger the workflow to destroy this
            // connection, so that a new request doesn't pick this closed connection.
            final Connection thisConnection = this;
            channel.closeFuture().addListener((ChannelFutureListener) future -> {
                logger.debug("OnChannelClose callback called for channel {}", channel);

                // if the closeFuture is not set, it means that closeAsync() wasn't called which means that the
                // close did not come from the client side. it means the server closed the channel for some reason.
                // it's important to distinguish that difference in debugging
                if (thisConnection.closeFuture.get() == null) {
                    logger.error(String.format(
                            "Server closed the Connection on channel %s - scheduling removal from %s",
                            channel.id().asShortText(), thisConnection.pool.getPoolInfo(thisConnection)));

                    // delegate the task to scheduler thread and free up the event loop
                    thisConnection.cluster.connectionScheduler().submit(() -> thisConnection.pool.definitelyDestroyConnection(thisConnection));
                }
            });

            logger.info("Created new connection for {}", uri);
        } catch (Exception ex) {
            throw new ConnectionException(uri, "Could not open " + getConnectionInfo(true), ex);
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

    public ChannelPromise write(final RequestMessageV4 requestMessage, final CompletableFuture<ResultSet> resultQueueSetup) {
        // dont allow the same request id to be used as one that is already in the queue
        if (pending.containsKey(requestMessage.getRequestId()))
            throw new IllegalStateException(String.format("There is already a request pending with an id of: %s", requestMessage.getRequestId()));

        // once there is a completed write, then create a traverser for the result set and complete
        // the promise so that the client knows that that it can start checking for results.
        final Connection thisConnection = this;

        final ChannelPromise requestPromise = channel.newPromise()
                .addListener(f -> {
                    if (!f.isSuccess()) {
                        if (logger.isDebugEnabled())
                            logger.debug(String.format("Write on connection %s failed",
                                    thisConnection.getConnectionInfo()), f.cause());

                        handleConnectionCleanupOnError(thisConnection);

                        cluster.executor().submit(() -> resultQueueSetup.completeExceptionally(f.cause()));
                    } else {
                        final LinkedBlockingQueue<Result> resultLinkedBlockingQueue = new LinkedBlockingQueue<>();
                        final CompletableFuture<Void> readCompleted = new CompletableFuture<>();

                        readCompleted.whenCompleteAsync((v, t) -> {
                            if (t != null) {
                                // the callback for when the read failed. a failed read means the request went to the server
                                // and came back with a server-side error of some sort.  it means the server is responsive
                                // so this isn't going to be like a potentially dead host situation which is handled above on a failed
                                // write operation.
                                logger.debug("Error while processing request on the server {}.", this, t);
                                handleConnectionCleanupOnError(thisConnection);
                            } else {
                                // the callback for when the read was successful, meaning that ResultQueue.markComplete()
                                // was called
                                thisConnection.returnToPool();
                            }
                            // While this request was in process, close might have been signaled in closeAsync().
                            // However, close would be blocked until all pending requests are completed. Attempt
                            // the shutdown if the returned result cleared up the last pending message and unblocked
                            // the close.
                            tryShutdown();
                        }, cluster.executor());

                        final ResultQueue handler = new ResultQueue(resultLinkedBlockingQueue, readCompleted);
                        pending.put(requestMessage.getRequestId(), handler);

                        // resultQueueSetup should only be completed by a worker since the application code might have sync
                        // completion stages attached to it which and we do not want the event loop threads to process those
                        // stages.
                        cluster.executor().submit(() -> resultQueueSetup.complete(
                                new ResultSet(handler, cluster.executor(), readCompleted, requestMessage, pool.host)));
                    }
                });
        channel.writeAndFlush(requestMessage, requestPromise);

        return requestPromise;
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
            // the session close message was removed in 3.5.0 after deprecation at 3.3.11. That removal was perhaps
            // a bit hasty as session semantics may still require this message in certain cases. Until we can look
            // at this in more detail, it seems best to bring back the old functionality to the driver.
            // TODO: commented due to not supporting sessions with HTTP.
            /*if (client instanceof Client.SessionedClient) {
                final boolean forceClose = client.getSettings().getSession().get().isForceClosed();
                final RequestMessageV4 closeMessage = client.buildMessage(
                        RequestMessageV4.build(Tokens.OPS_CLOSE).addArg(Tokens.ARGS_FORCE, forceClose)).create();

                final CompletableFuture<ResultSet> closed = new CompletableFuture<>();

                // TINKERPOP-2822 should investigate this write more carefully to check for sensible behavior
                // in the event the Channel was not created but we try to send the close message
                write(closeMessage, closed);

                try {
                    // make sure we get a response here to validate that things closed as expected.  on error, we'll let
                    // the server try to clean up on its own.  the primary error here should probably be related to
                    // protocol issues which should not be something a user has to fuss with.
                    closed.join().all().get(cluster.getMaxWaitForClose(), TimeUnit.MILLISECONDS);
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
            }*/

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
                        getChannelId(), pool.host.toString(), isDead(), this.borrowed.get(), getPending().size(), this.isBeingReplaced, isClosing(), createdTimestamp, creatingThread) :
                String.format("Connection{channel=%s isDead=%s borrowed=%s pending=%s markedReplaced=%s closing=%s created=%s thread=%s}",
                        getChannelId(), isDead(), this.borrowed.get(), getPending().size(), this.isBeingReplaced, isClosing(), createdTimestamp, creatingThread);
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
