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

import io.netty.handler.codec.CodecException;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
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

    public static final int MAX_IN_PROCESS = 4;
    public static final int MIN_IN_PROCESS = 1;
    public static final int MAX_WAIT_FOR_CONNECTION = 3000;
    public static final int MAX_WAIT_FOR_SESSION_CLOSE = 3000;
    public static final int MAX_CONTENT_LENGTH = 65536;
    public static final int RECONNECT_INITIAL_DELAY = 1000;
    public static final int RECONNECT_INTERVAL = 1000;
    public static final int RESULT_ITERATION_BATCH_SIZE = 64;

    /**
     * When a {@code Connection} is borrowed from the pool, this number is incremented to indicate the number of
     * times it has been taken and is decremented when it is returned.  This number is one indication as to how
     * busy a particular {@code Connection} is.
     */
    public final AtomicInteger borrowed = new AtomicInteger(0);
    private final AtomicReference<Class<Channelizer>> channelizerClass = new AtomicReference<>(null);

    private volatile boolean isDead = false;
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

        connectionLabel = String.format("Connection{host=%s}", pool.host);

        if (cluster.isClosing()) throw new IllegalStateException("Cannot open a connection while the cluster after close() is called");

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

            logger.info("Created new connection for {}", uri);
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

    public boolean isDead() {
        return isDead;
    }

    public boolean isClosed() {
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

    public CompletableFuture<Void> closeAsync() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        if (!closeFuture.compareAndSet(null, future))
            return closeFuture.get();

        // make sure all requests in the queue are fully processed before killing.  if they are then shutdown
        // can be immediate.  if not this method will signal the readCompleted future defined in the write()
        // operation to check if it can close.  in this way the connection no longer receives writes, but
        // can continue to read. If a request never comes back the future won't get fulfilled and the connection
        // will maintain a "pending" request, that won't quite ever go away.  The build up of such a dead requests
        // on a connection in the connection pool will force the pool to replace the connection for a fresh one
        if (pending.isEmpty()) {
            if (null == channel)
                future.complete(null);
            else
                shutdown(future);
        }

        return future;
    }

    public void close() {
        try {
            closeAsync().get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ChannelPromise write(final RequestMessage requestMessage, final CompletableFuture<ResultSet> future) {
        // once there is a completed write, then create a traverser for the result set and complete
        // the promise so that the client knows that that it can start checking for results.
        final Connection thisConnection = this;
        final ChannelPromise promise = channel.newPromise()
                .addListener(f -> {
                    if (!f.isSuccess()) {
                        if (logger.isDebugEnabled())
                            logger.debug(String.format("Write on connection %s failed", thisConnection.getConnectionInfo()), f.cause());
                        thisConnection.isDead = true;
                        thisConnection.returnToPool();
                        future.completeExceptionally(f.cause());
                    } else {
                        final LinkedBlockingQueue<Result> resultLinkedBlockingQueue = new LinkedBlockingQueue<>();
                        final CompletableFuture<Void> readCompleted = new CompletableFuture<>();

                        // the callback for when the read was successful, meaning that ResultQueue.markComplete()
                        // was called
                        readCompleted.thenAcceptAsync(v -> {
                            thisConnection.returnToPool();
                            tryShutdown();
                        }, cluster.executor());

                        // the callback for when the read failed. a failed read means the request went to the server
                        // and came back with a server-side error of some sort.  it means the server is responsive
                        // so this isn't going to be like a dead host situation which is handled above on a failed
                        // write operation.
                        //
                        // in the event of an IOException (typically means that the Connection might have
                        // been closed from the server side - this is typical in situations like when a request is
                        // sent that exceeds maxContentLength and the server closes the channel on its side) or other
                        // exceptions that indicate a non-recoverable state for the Connection object
                        // (a netty CorruptedFrameException is a good example of that), the Connection cannot simply
                        // be returned to the pool as future uses will end with refusal from the server and make it
                        // appear as a dead host as the write will not succeed. instead, the Connection needs to be
                        // replaced in these scenarios which destroys the dead channel on the client and allows a new
                        // one to be reconstructed.
                        readCompleted.exceptionally(t -> {
                            if (t instanceof IOException || t instanceof CodecException) {
                                if (pool != null) pool.replaceConnection(thisConnection);
                            } else {
                                thisConnection.returnToPool();
                            }

                            // close was signaled in closeAsync() but there were pending messages at that time. attempt
                            // the shutdown if the returned result cleared up the last pending message
                            tryShutdown();

                            return null;
                        });

                        final ResultQueue handler = new ResultQueue(resultLinkedBlockingQueue, readCompleted);
                        pending.put(requestMessage.getRequestId(), handler);
                        future.complete(new ResultSet(handler, cluster.executor(), readCompleted));
                    }
                });
        channel.writeAndFlush(requestMessage, promise);

        return promise;
    }

    public void returnToPool() {
        try {
            if (pool != null) pool.returnConnection(this);
        } catch (ConnectionException ce) {
            if (logger.isDebugEnabled())
                logger.debug("Returned {} connection to {} but an error occurred - {}", this.getConnectionInfo(), pool, ce.getMessage());
        }
    }

    /**
     * Close was signaled in closeAsync() but there were pending messages at that time. This method attempts the
     * shutdown if the returned result cleared up the last pending message.
     */
    private void tryShutdown() {
        if (isClosed() && pending.isEmpty())
            shutdown(closeFuture.get());
    }

    private void shutdown(final CompletableFuture<Void> future) {
        // shutdown can be called directly from closeAsync() or after write() and therefore this method should only
        // be called once. once shutdown is initiated, it shouldn't be executed a second time or else it sends more
        // messages at the server and leads to ugly log messages over there.
        if (shutdownInitiated.compareAndSet(false, true)) {
            if (client instanceof Client.SessionedClient) {
                // maybe this should be delegated back to the Client implementation???
                final RequestMessage closeMessage = client.buildMessage(RequestMessage.build(Tokens.OPS_CLOSE)).create();
                final CompletableFuture<ResultSet> closed = new CompletableFuture<>();
                write(closeMessage, closed);

                try {
                    // make sure we get a response here to validate that things closed as expected.  on error, we'll let
                    // the server try to clean up on its own.  the primary error here should probably be related to
                    // protocol issues which should not be something a user has to fuss with.
                    closed.get(cluster.connectionPoolSettings().maxWaitForSessionClose, TimeUnit.MILLISECONDS);
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
                if (f.cause() != null)
                    future.completeExceptionally(f.cause());
                else
                    future.complete(null);
            });

            channel.close(promise);
        }
    }

    public String getConnectionInfo() {
        return String.format("Connection{host=%s, isDead=%s, borrowed=%s, pending=%s}",
                pool.host, isDead, borrowed, pending.size());
    }

    @Override
    public String toString() {
        return connectionLabel;
    }
}
