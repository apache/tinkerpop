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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Connection pool combines two entities. One is the underlying Netty channel pool and another is
 * the Connection whose lifetime is synonymous with a request.
 */
public class ConnectionPoolImpl implements ConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPoolImpl.class);
    private final Host host;
    private final Cluster cluster;
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>(null);

    /**
     * Netty's implementation of channel management with an upper bound. This connection
     * pool is responsible for attaching a channel with each request.
     */
    private FixedChannelPool channelPool;
    /**
     * Channel initializer that is safe to be re-used across multiple channels.
     */
    private Channelizer channelizer;

    /**
     * Keeps track of all the active channels. Closed channels are automatically removed
     * from the group.
     */
    private final ChannelGroup activeChannels;

    /**
     * Create and initializes the connection pool
     *
     * @return A connection pool which has initialized its internal implementation.
     */
    public static ConnectionPool create(final Host host, final Cluster cluster) {
        final ConnectionPoolImpl connPool = new ConnectionPoolImpl(host, cluster);
        connPool.init();

        logger.info("Created {}", connPool);

        return connPool;
    }

    private ConnectionPoolImpl(final Host host, final Cluster cluster) {
        this.host = host;
        this.cluster = cluster;
        this.activeChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    }

    private void init() {
        Class<Channelizer> channelizerClass = null;
        try {
            channelizerClass = (Class<Channelizer>) Class.forName(cluster.connectionPoolSettings().channelizer);
            this.channelizer = channelizerClass.newInstance();
            this.channelizer.init(this);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Error while initializing channelizer " +
                                                       (channelizerClass != null ? channelizerClass : "NULL"));
        }

        final Bootstrap b = cluster.getFactory().createBootstrap();
        b.remoteAddress(host.getHostUri().getHost(), host.getHostUri().getPort());
        // TODO: Use Epoll if available
        b.channel(NioSocketChannel.class);

        final ChannelPoolHandler handler = new ChannelPoolHandler() {
            @Override
            public void channelReleased(final Channel ch) {
                // Note: Any operation performed here might have direct impact on the performance of the
                // client since, this method is called with every new request.
                if (logger.isDebugEnabled())
                    logger.debug("Channel released: {}", ch);
            }

            @Override
            public void channelAcquired(final Channel ch) {
                // Note: Any operation performed here might have direct impact on the performance of the
                // client since, this method is called with every new request.
                if (logger.isDebugEnabled())
                    logger.debug("Channel acquired: {}", ch);
            }

            @Override
            public void channelCreated(final Channel ch) {
                if (logger.isDebugEnabled())
                    logger.debug("Channel created: {}", ch);
                // Guaranteed that it is a socket channel because we set b.channel as SocketChannel
                final SocketChannel sch = (SocketChannel) ch;
                ((Channelizer.AbstractChannelizer) channelizer).initChannel(sch);
            }
        };

        this.channelPool = createChannelPool(b, cluster.connectionPoolSettings(), handler);

        if (logger.isDebugEnabled())
            logger.debug("Initialized {} successfully.", this);
    }

    private FixedChannelPool createChannelPool(final Bootstrap b,
                                               final Settings.ConnectionPoolSettings connectionPoolSettings,
                                               final ChannelPoolHandler handler) {
        return new FixedChannelPool(b,
                                    handler,
                                    ChannelHealthChecker.ACTIVE,
                                    FixedChannelPool.AcquireTimeoutAction.FAIL, // throw an exception on acquire timeout
                                    connectionPoolSettings.maxWaitForConnection,
                                    connectionPoolSettings.maxSize, /*maxConnections*/
                  1, /*maxPendingAcquires*/
                   true);/*releaseHealthCheck*/
    }

    @Override
    public ChannelGroup getActiveChannels() {
        return this.activeChannels;
    }

    @Override
    public CompletableFuture<Void> releaseConnection(final Connection connection) {
        final Channel channelAssociatedToConnection = connection.getChannel();

        final CompletableFuture<Void> future = new CompletableFuture<>();
        final Promise<Void> promise = channelAssociatedToConnection.newPromise().addListener(f -> {
            if (f.isDone()) {
                future.complete(null);
            } else {
                future.completeExceptionally(f.cause());
            }
        });

        this.channelPool.release(channelAssociatedToConnection, promise);

        return future;
    }

    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture.get() != null) return closeFuture.get(); // Make this API idempotent

        final CompletableFuture activeChannelClosedFuture = CompletableFuture.runAsync(() -> {
            logger.info("Closing active channels borrowed from ChannelPool [BusyConnectionCount={}]", this.getActiveChannels().size());
            this.activeChannels.close().syncUninterruptibly();
            logger.debug("Closed all active channels.");
        });

        final CompletableFuture<Void> channelPoolClosedFuture = new CompletableFuture<>();
        this.channelPool.closeAsync().addListener((f) -> {
            if (f.isSuccess()) {
                logger.debug("Closed underlying ChannelPool {}", this.channelPool);
                channelPoolClosedFuture.complete(null);
            } else {
                logger.error("ChannelPool did not close gracefully", f.cause());
                channelPoolClosedFuture.completeExceptionally(f.cause());
            }
        });

        closeFuture.set(CompletableFuture.allOf(channelPoolClosedFuture, activeChannelClosedFuture)
                                         .thenRun(() -> logger.info("Closed {}", this)));

        return closeFuture.get();
    }

    @Override
    public ScheduledExecutorService executor() {
        return this.cluster.executor();
    }

    @Override
    public Host getHost() {
        return this.host;
    }

    @Override
    public Connection prepareConnection() throws TimeoutException, ConnectException {
        if (closeFuture.get() != null) {
            throw new RuntimeException(this + " is closing. Cannot borrow connection.");
        }

        // Get a channel, verify handshake is done and then attach it to a connectionPool
        final Channel ch = this.channelPool.acquire().syncUninterruptibly().getNow();

        // TODO: This call is un-necessary on every channel acquire, since handshake is done once.
        channelizer.connected(ch);

        return new SingleRequestConnection(ch, this);
    }

    @Override
    public Cluster getCluster() {
        return this.cluster;
    }

    @Override
    public String toString() {
        return String.format("ConnectionPool{closing=%s, host=%s, BusyConnectionCount=%d}",
                             (closeFuture.get() != null),
                             host,
                             this.channelPool.acquiredChannelCount());
    }
}
