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
package org.apache.tinkerpop.gremlin.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.commons.lang3.SystemUtils;
import org.apache.tinkerpop.gremlin.server.util.LifeCycleHook;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.server.util.ThreadFactoryUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Start and stop Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServer {

    static {
        // hook slf4j up to netty internal logging
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    private static final String SERVER_THREAD_PREFIX = "gremlin-server-";
    public static final String AUDIT_LOGGER_NAME = "audit.org.apache.tinkerpop.gremlin.server";

    private static final Logger logger = LoggerFactory.getLogger(GremlinServer.class);
    private final Settings settings;

    /**
     * The {@code ServerSocketChannel} established when binding the server to the host/port.
     */
    private Channel serverSocketChannel;

    private CompletableFuture<Void> serverStopped = null;
    private CompletableFuture<ServerGremlinExecutor> serverStarted = null;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ExecutorService gremlinExecutorService;
    private final ServerGremlinExecutor serverGremlinExecutor;
    private final boolean isEpollEnabled;
    private Channelizer channelizer;

    /**
     * Construct a Gremlin Server instance from {@link Settings}.
     */
    public GremlinServer(final Settings settings) {
        this(settings, null);
    }

    /**
     * Construct a Gremlin Server instance from {@link Settings} and {@link ExecutorService}.
     * This constructor is useful when Gremlin Server is being used in an embedded style
     * and there is a need to share thread pools with the hosting application.
     */
    public GremlinServer(final Settings settings, final ExecutorService gremlinExecutorService) {
        settings.optionalMetrics().ifPresent(GremlinServer::configureMetrics);
        this.settings = settings;
        provideDefaultForGremlinPoolSize(settings);
        this.isEpollEnabled = settings.useEpollEventLoop && SystemUtils.IS_OS_LINUX;
        if (settings.useEpollEventLoop && !SystemUtils.IS_OS_LINUX){
            logger.warn("cannot use epoll in non-linux env, falling back to NIO");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> this.stop().join(), SERVER_THREAD_PREFIX + "shutdown"));

        final ThreadFactory threadFactoryBoss = ThreadFactoryUtil.create("boss-%d");
        // if linux os use epoll else fallback to nio based eventloop
        // epoll helps in reducing GC and has better  performance
        // http://netty.io/wiki/native-transports.html
        if (isEpollEnabled){
            bossGroup = new EpollEventLoopGroup(settings.threadPoolBoss, threadFactoryBoss);
        } else {
            bossGroup = new NioEventLoopGroup(settings.threadPoolBoss, threadFactoryBoss);
        }

        final ThreadFactory threadFactoryWorker = ThreadFactoryUtil.create("worker-%d");
        if (isEpollEnabled) {
            workerGroup = new EpollEventLoopGroup(settings.threadPoolWorker, threadFactoryWorker);
        } else {
            workerGroup = new NioEventLoopGroup(settings.threadPoolWorker, threadFactoryWorker);
        }

        // use the ExecutorService returned from ServerGremlinExecutor as it might be initialized there
        serverGremlinExecutor = new ServerGremlinExecutor(settings, gremlinExecutorService, workerGroup);
        this.gremlinExecutorService = serverGremlinExecutor.getGremlinExecutorService();
    }

    /**
     * Start Gremlin Server with {@link Settings} provided to the constructor.
     */
    public synchronized CompletableFuture<ServerGremlinExecutor> start() throws Exception {
        if (serverStarted != null) {
            // server already started - don't get it rolling again
            return serverStarted;
        }

        serverStarted = new CompletableFuture<>();
        final CompletableFuture<ServerGremlinExecutor> serverReadyFuture = serverStarted;
        try {
            final ServerBootstrap b = new ServerBootstrap();

            // when high value is reached then the channel becomes non-writable and stays like that until the
            // low value is so that there is time to recover
            b.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                    new WriteBufferWaterMark(settings.writeBufferLowWaterMark, settings.writeBufferHighWaterMark));
            b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            // Enable TCP Keep-Alive to detect if the remote peer is still reachable.
            // Keep-Alive sends periodic probes to check if the remote peer is still active.
            // If the remote peer is unreachable, the connection will be closed, preventing
            // resource leaks and avoiding the maintenance of stale connections.
            b.childOption(ChannelOption.SO_KEEPALIVE, true);

            // fire off any lifecycle scripts that were provided by the user. hooks get initialized during
            // ServerGremlinExecutor initialization
            serverGremlinExecutor.getHooks().forEach(hook -> {
                logger.info("Executing start up {}", LifeCycleHook.class.getSimpleName());
                try {
                    hook.onStartUp(new LifeCycleHook.Context(logger));
                } catch (UnsupportedOperationException uoe) {
                    // if the user doesn't implement onStartUp the scriptengine will throw
                    // this exception.  it can safely be ignored.
                }
            });

            channelizer = createChannelizer(settings);
            channelizer.init(serverGremlinExecutor);
            b.group(bossGroup, workerGroup)
                    .childHandler(channelizer);
            if (isEpollEnabled){
                b.channel(EpollServerSocketChannel.class);
            } else{
                b.channel(NioServerSocketChannel.class);
            }

            // bind to host/port and wait for ServerSocketChannel to be ready
            b.bind(settings.host, settings.port).addListener((ChannelFutureListener) channelFuture -> {
                if (channelFuture.isSuccess()) {
                    serverSocketChannel = channelFuture.channel();

                    logger.info("Gremlin Server configured with worker thread pool of {}, gremlin pool of {} and boss thread pool of {}.",
                            settings.threadPoolWorker, settings.gremlinPool, settings.threadPoolBoss);
                    logger.info("Channel started at port {}.", settings.port);

                    serverReadyFuture.complete(serverGremlinExecutor);
                } else {
                    serverReadyFuture.completeExceptionally(new IOException(
                            String.format("Could not bind to %s and %s - perhaps something else is bound to that address.", settings.host, settings.port)));
                }
            });
        } catch (Exception ex) {
            logger.error("Gremlin Server Error", ex);
            serverReadyFuture.completeExceptionally(ex);
        }

        return serverStarted;
    }

    private static Channelizer createChannelizer(final Settings settings) throws Exception {
        try {
            final Class clazz = Class.forName(settings.channelizer);
            final Object o = clazz.newInstance();

            final Channelizer c = (Channelizer) o;
            if (c.supportsIdleMonitor()) {
                logger.info("idleConnectionTimeout was set to {} which resolves to {} seconds when configuring this value - this feature will be {}",
                        settings.idleConnectionTimeout, settings.idleConnectionTimeout / 1000, settings.idleConnectionTimeout < 1000 ? "disabled" : "enabled");
                logger.info("keepAliveInterval was set to {} which resolves to {} seconds when configuring this value - this feature will be {}",
                        settings.keepAliveInterval, settings.keepAliveInterval / 1000, settings.keepAliveInterval < 1000 ? "disabled" : "enabled");
            }

            return c;
        } catch (ClassNotFoundException cnfe) {
            logger.error("Could not find {} implementation defined by the 'channelizer' setting as: {}",
                    Channelizer.class.getName(), settings.channelizer);
            throw new RuntimeException(cnfe);
        } catch (Exception ex) {
            logger.error("Class defined by the 'channelizer' setting as: {} could not be properly instantiated as a {}",
                    settings.channelizer, Channelizer.class.getName());
            throw new RuntimeException(ex);
        }
    }

    /**
     * Stop Gremlin Server and free the port binding. Note that multiple calls to this method will return the
     * same instance of the {@code CompletableFuture}.
     */
    public synchronized CompletableFuture<Void> stop() {
        if (serverStopped != null) {
            // shutdown has started so don't fire it off again
            return serverStopped;
        }

        serverStopped = new CompletableFuture<>();
        final CountDownLatch servicesLeftToShutdown = new CountDownLatch(3);

        // it's possible that a channel might not be initialized in the first place if bind() fails because
        // of port conflict.  in that case, there's no need to wait for the channel to close.
        if (null == serverSocketChannel)
            servicesLeftToShutdown.countDown();
        else
            serverSocketChannel.close().addListener(f -> servicesLeftToShutdown.countDown());

        logger.info("Shutting down thread pools.");

        try {
            if (gremlinExecutorService != null) gremlinExecutorService.shutdown();
        } finally {
            logger.debug("Shutdown Gremlin thread pool.");
        }

        try {
            workerGroup.shutdownGracefully().addListener((GenericFutureListener) f -> servicesLeftToShutdown.countDown());
        } finally {
            logger.debug("Shutdown Worker thread pool.");
        }
        try {
            bossGroup.shutdownGracefully().addListener((GenericFutureListener) f -> servicesLeftToShutdown.countDown());
        } finally {
            logger.debug("Shutdown Boss thread pool.");
        }

        // channel is shutdown as are the thread pools - time to kill graphs as nothing else should be acting on them
        new Thread(() -> {
            if (serverGremlinExecutor != null) {
                serverGremlinExecutor.getHooks().forEach(hook -> {
                    logger.info("Executing shutdown {}", LifeCycleHook.class.getSimpleName());
                    try {
                        hook.onShutDown(new LifeCycleHook.Context(logger));
                    } catch (UnsupportedOperationException | UndeclaredThrowableException uoe) {
                        // if the user doesn't implement onShutDown the scriptengine will throw
                        // this exception.  it can safely be ignored.
                    }
                });
            }

            try {
                if (gremlinExecutorService != null) {
                    if (!gremlinExecutorService.awaitTermination(30000, TimeUnit.MILLISECONDS)) {
                        logger.warn("Gremlin thread pool did not fully terminate - continuing with shutdown process");
                    }
                }
            } catch (InterruptedException ie) {
                logger.warn("Timeout waiting for Gremlin thread pool to shutdown - continuing with shutdown process.");
            }

            try {
                servicesLeftToShutdown.await(30000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                logger.warn("Timeout waiting for boss/worker thread pools to shutdown - continuing with shutdown process.");
            }

            // close TraversalSource and Graph instances - there aren't guarantees that closing Graph will close all
            // spawned TraversalSource instances so both should be closed directly and independently.
            if (serverGremlinExecutor != null) {
                final Set<String> traversalSourceNames = serverGremlinExecutor.getGraphManager().getTraversalSourceNames();
                traversalSourceNames.forEach(traversalSourceName -> {
                    logger.debug("Closing GraphTraversalSource instance [{}]", traversalSourceName);
                    try {
                        serverGremlinExecutor.getGraphManager().getTraversalSource(traversalSourceName).close();
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception while closing GraphTraversalSource instance [%s]", traversalSourceName), ex);
                    } finally {
                        logger.info("Closed GraphTraversalSource instance [{}]", traversalSourceName);
                    }

                    try {
                        serverGremlinExecutor.getGraphManager().removeTraversalSource(traversalSourceName);
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception while removing GraphTraversalSource instance [%s] from GraphManager", traversalSourceName), ex);
                    }
                });

                final Set<String> graphNames = serverGremlinExecutor.getGraphManager().getGraphNames();
                graphNames.forEach(gName -> {
                    logger.debug("Closing Graph instance [{}]", gName);
                    try {
                        final Graph graph = serverGremlinExecutor.getGraphManager().getGraph(gName);
                        graph.close();
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception while closing Graph instance [%s]", gName), ex);
                    } finally {
                        logger.info("Closed Graph instance [{}]", gName);
                    }

                    try {
                        serverGremlinExecutor.getGraphManager().removeGraph(gName);
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception while removing Graph instance [%s] from GraphManager", gName), ex);
                    }
                });
            }

            // kills reporter threads. this is a last bit of cleanup that can be done. typically, the jvm is headed
            // for shutdown which would obviously kill the reporters, but when it isn't they just keep reporting.
            // removing them all will silent them up and release the appropriate resources.
            MetricManager.INSTANCE.removeAllReporters();

            // removing all the metrics should allow Gremlin Server to clean up the metrics instance so that it can be
            // started again in the same JVM without those metrics initialized which generates a warning and won't
            // reset to start values
            MetricManager.INSTANCE.removeAllMetrics();

            logger.info("Gremlin Server - shutdown complete");
            serverStopped.complete(null);
        }, SERVER_THREAD_PREFIX + "stop").start();

        return serverStopped;
    }

    public ServerGremlinExecutor getServerGremlinExecutor() {
        return serverGremlinExecutor;
    }

    public Channelizer getChannelizer() {
        return channelizer;
    }

    public static void main(final String[] args) throws Exception {
        // add to vm options: -Dlogback.configurationFile=file:conf/logback.xml
        printHeader();
        final String file;
        if (args.length > 0)
            file = args[0];
        else
            file = "conf/gremlin-server.yaml";

        final Settings settings;
        try {
            settings = Settings.read(file);
        } catch (Exception ex) {
            logger.error("Configuration file at {} could not be found or parsed properly. [{}]", file, ex.getMessage());
            return;
        }

        logger.info("Configuring Gremlin Server from {}", file);
        final GremlinServer server = new GremlinServer(settings);
        server.start().exceptionally(t -> {
            logger.error("Gremlin Server was unable to start and will now begin shutdown: {}", t.getMessage());
            server.stop().join();
            return null;
        }).join();
    }

    public static String getHeader() {
        final StringBuilder builder = new StringBuilder();
        builder.append(Gremlin.version() + "\r\n");
        builder.append("         \\,,,/\r\n");
        builder.append("         (o o)\r\n");
        builder.append("-----oOOo-(" + Gremlin.majorVersion() + ")-oOOo-----\r\n");
        return builder.toString();
    }

    private static void configureMetrics(final Settings.ServerMetrics settings) {
        final MetricManager metrics = MetricManager.INSTANCE;
        settings.optionalConsoleReporter().ifPresent(config -> {
            if (config.enabled) metrics.addConsoleReporter(config.interval);
        });

        settings.optionalCsvReporter().ifPresent(config -> {
            if (config.enabled) metrics.addCsvReporter(config.interval, config.fileName);
        });

        settings.optionalJmxReporter().ifPresent(config -> {
            if (config.enabled) metrics.addJmxReporter(config.domain, config.agentId);
        });

        settings.optionalSlf4jReporter().ifPresent(config -> {
            if (config.enabled) metrics.addSlf4jReporter(config.interval, config.loggerName);
        });

        settings.optionalGangliaReporter().ifPresent(config -> {
            if (config.enabled) {
                try {
                    metrics.addGangliaReporter(config.host, config.port,
                            config.addressingMode, config.ttl, config.protocol31, config.hostUUID, config.spoof, config.interval);
                } catch (IOException ioe) {
                    logger.warn("Error configuring the Ganglia Reporter.", ioe);
                }
            }
        });

        settings.optionalGraphiteReporter().ifPresent(config -> {
            if (config.enabled) metrics.addGraphiteReporter(config.host, config.port, config.prefix, config.interval);
        });
    }

    private static void printHeader() {
        logger.info(getHeader());
    }

    private static void provideDefaultForGremlinPoolSize(final Settings settings) {
        if (settings.gremlinPool == 0)
            settings.gremlinPool = Runtime.getRuntime().availableProcessors();
    }

    @Override
    public String toString() {
        return "GremlinServer " + settings.host + ":" + settings.port;
    }
}
