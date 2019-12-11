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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;

import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Connection that can be used to submit only a single request.
 */
public class SingleRequestConnection implements Connection {
    /*
     * NOTE: An instance of this class is created for every request. Keep the member variables of this class lean and thin to
     * avoid creating excess objects on the heap.
     */
    private static final Logger logger = LoggerFactory.getLogger(SingleRequestConnection.class);

    private final Channel channel;
    private final ConnectionPool pool;

    private CompletableFuture<ResultSet> resultFuture;
    private ChannelPromise requestPromise;

    /**
     * Future that indicates the release of the underlying resources (like the channel) for this connection.
     */
    private AtomicReference<CompletableFuture<Void>> releaseFuture;

    static final AttributeKey<ResultQueue> RESULT_QUEUE_ATTRIBUTE_KEY = AttributeKey.newInstance("resultQueueFuture");

    SingleRequestConnection(final Channel channel, final ConnectionPool pool) {
        // A channel is attached with a request only when the channel is active. This is the responsibility
        // of channelpool to ensure that the channel attached to this connection is healthy. Something is fishy
        // if this is not true, hence, IllegalState.
        if (!channel.isActive()) {
            throw new IllegalStateException("Channel " + channel + " is not active.");
        }

        this.channel = channel;
        this.pool = pool;
        this.releaseFuture = new AtomicReference<>(null);
    }

    private void onChannelWriteError(final CompletableFuture<ResultSet> resultQueueSetup, final Throwable cause) {
        if (cause instanceof ClosedChannelException) {
            resultQueueSetup.completeExceptionally(new ConnectException("Failed to connect to the server. Check the server connectivity."));
        } else {
            resultQueueSetup.completeExceptionally(cause);
        }

        logger.debug("Write to the channel failed. {} may be dead. Returning to pool for replacement.", this, cause);

        this.releaseResources();
    }

    private void onResultReadCompleteError(final Throwable cause) {
        if (cause instanceof InterruptedException) {
            logger.debug("Forcing close of {}.", this, cause);
            // HACK: There is no good way to signal to the server that request is complete
            // so that it can release resources. As a nuke option, close the channel itself.
            this.forceTerminate();
        } else if (cause instanceof ResponseException) {
            logger.debug("Error while processing request on the server {}.", this, cause);
            this.releaseResources();
        } else {
            // This could be a case when a connected channel processing a request has been closed
            // from the client side.
            logger.debug("Error while reading the response from the server for {}.", this, cause);
            this.releaseResources();
        }
    }

    private void onResultReadCompleteSuccess() {
        // return to pool on successful completion of reading the results
        this.releaseResources();
    }

    /**
     * Force an abnormal close of a connection. This is accomplished by closing the underlying Netty {@link Channel}.
     */
    private void forceTerminate() {
        this.channel.close();

        this.channel.closeFuture().addListener(f -> {
            if (!f.isSuccess()) {
                logger.warn("Failed to closed channel {}. This might lead to a connection leak.", channel, f.cause());
            }

            this.pool.executor().submit(this::releaseResources);
        });
    }

    /**
     * Closes the connection gracefully by releasing the resources and notifying the appropriate
     * listener. This is an idempotent API.
     *
     * @return Future that represents a successful release of all the resources
     */
    synchronized CompletableFuture<Void> releaseResources() {
        // make this operation idempotent
        if (this.releaseFuture.get() != null) {
            return releaseFuture.get();
        }

        // Remove the result queue from the channel
        if (channel.hasAttr(RESULT_QUEUE_ATTRIBUTE_KEY) && (channel.attr(RESULT_QUEUE_ATTRIBUTE_KEY).get() != null)) {
            final ResultQueue resultQueue = channel.attr(RESULT_QUEUE_ATTRIBUTE_KEY).getAndSet(null);
            if (!resultQueue.isComplete()) {
                // resultQueue should have been completely either successfully or exceptionally by now by the channel handler.
                resultQueue.markError(new IllegalStateException("Failed to read results. Connection to the server is closed."));
            }
        }

        releaseFuture.compareAndSet(null, this.pool.releaseConnection(this));

        return releaseFuture.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ChannelPromise write(final RequestMessage requestMessage, final CompletableFuture<ResultSet> resultQueueSetup) {
        if (this.resultFuture != null)
            throw new IllegalStateException("This " + this + " is already in use. Cannot reuse it for request " + requestMessage);

        this.resultFuture = resultQueueSetup;

        final CompletableFuture<Void> readCompleted = new CompletableFuture<>();
        readCompleted.whenCompleteAsync((v, t) -> {
            if (t != null) {
                this.onResultReadCompleteError(t);
            } else {
                this.onResultReadCompleteSuccess();
            }
        }, pool.executor());

        // As a further optimization this creation could be done on successful write to the server.
        final LinkedBlockingQueue<Result> resultLinkedBlockingQueue = new LinkedBlockingQueue<>();
        final ResultQueue queue = new ResultQueue(resultLinkedBlockingQueue, readCompleted);
        queue.setChannelId(channel.id().asLongText());
        if (!channel.attr(RESULT_QUEUE_ATTRIBUTE_KEY).compareAndSet(null, queue)) {
            throw new IllegalStateException("Channel " + channel + " already has a result queue attached to it");
        }

        final SingleRequestConnection thisSingleRequestConnection = this;
        requestPromise = channel.newPromise()
                                .addListener(f -> {
                                    // Delegate event handling to workers after I/O to free up EventLoopThreads
                                    if (!f.isSuccess()) {
                                        pool.executor().submit(() -> thisSingleRequestConnection.onChannelWriteError(resultQueueSetup, f.cause()));
                                    } else {
                                        // resultQueueSetup should only be completed by a worker since the application code might have sync
                                        // completion stages attached to which and we do not want the event loop threads to process those
                                        // stages.
                                        pool.executor().submit(() -> resultQueueSetup.complete(new ResultSet(queue,
                                                                                                             pool.executor(),
                                                                                                             readCompleted,
                                                                                                             requestMessage,
                                                                                                             thisSingleRequestConnection.getHost())));
                                    }
                                });

        channel.writeAndFlush(requestMessage, requestPromise);

        return requestPromise;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Channel getChannel() {
        return this.channel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Host getHost() {
        return this.pool.getHost();
    }

    @Override
    public String toString() {
        return String.format("Connection{host=%s,channel=%s}", pool.getHost(), channel);
    }
}