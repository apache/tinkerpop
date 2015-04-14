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

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Start and stop Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServer {

    static {
        // hook slf4j up to netty internal logging
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    private static final String SERVER_THREAD_PREFIX = "gremlin-server-";

    private static final Logger logger = LoggerFactory.getLogger(GremlinServer.class);
    private final Settings settings;
    private Optional<Graphs> graphs = Optional.empty();
    private Channel ch;

    private CompletableFuture<Void> serverStopped = null;
    private CompletableFuture<Void> serverStarted = null;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ExecutorService gremlinExecutorService;

    public GremlinServer(final Settings settings) {
        this.settings = settings;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> this.stop().join(), SERVER_THREAD_PREFIX + "shutdown"));

        final BasicThreadFactory threadFactoryBoss = new BasicThreadFactory.Builder().namingPattern(SERVER_THREAD_PREFIX + "boss-%d").build();
        bossGroup = new NioEventLoopGroup(settings.threadPoolBoss, threadFactoryBoss);

        final BasicThreadFactory threadFactoryWorker = new BasicThreadFactory.Builder().namingPattern(SERVER_THREAD_PREFIX + "worker-%d").build();
        workerGroup = new NioEventLoopGroup(settings.threadPoolWorker, threadFactoryWorker);

        final BasicThreadFactory threadFactoryGremlin = new BasicThreadFactory.Builder().namingPattern(SERVER_THREAD_PREFIX + "exec-%d").build();
        gremlinExecutorService = Executors.newFixedThreadPool(settings.gremlinPool, threadFactoryGremlin);
    }

    /**
     * Start Gremlin Server with {@link Settings} provided to the constructor.
     */
    public synchronized CompletableFuture<Void> start() throws Exception {
        if(serverStarted != null) {
            // server already started - don't get it rolling again
            return serverStarted;
        }

        serverStarted = new CompletableFuture<>();
        final CompletableFuture<Void> serverReadyFuture = serverStarted = new CompletableFuture<>();
        try {
            final ServerBootstrap b = new ServerBootstrap();

            // when high value is reached then the channel becomes non-writable and stays like that until the
            // low value is so that there is time to recover
            b.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, settings.writeBufferLowWaterMark);
            b.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, settings.writeBufferHighWaterMark);
            b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            final GremlinExecutor gremlinExecutor = initializeGremlinExecutor(gremlinExecutorService, workerGroup);
            final Channelizer channelizer = createChannelizer(settings);
            channelizer.init(settings, gremlinExecutor, gremlinExecutorService, graphs.get(), workerGroup);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(channelizer);

            // bind to host/port and wait for channel to be ready
            b.bind(settings.host, settings.port).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(final ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()) {
                        ch = channelFuture.channel();

                        logger.info("Gremlin Server configured with worker thread pool of {}, gremlin pool of {} and boss thread pool of {}.",
                                settings.threadPoolWorker, settings.gremlinPool, settings.threadPoolBoss);
                        logger.info("Channel started at port {}.", settings.port);

                        serverReadyFuture.complete(null);
                    } else {
                        serverReadyFuture.completeExceptionally(new IOException(
                                String.format("Could not bind to %s and %s - perhaps something else is bound to that address.", settings.host, settings.port)));
                    }
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
            return (Channelizer) o;
        } catch (ClassNotFoundException fnfe) {
            logger.error("Could not find {} implementation defined by the 'channelizer' setting as: {}",
                    Channelizer.class.getName(), settings.channelizer);
            throw new RuntimeException(fnfe);
        } catch (Exception ex) {
            logger.error("Class defined by the 'channelizer' setting as: {} could not be properly instantiated as a {}",
                    settings.channelizer, Channelizer.class.getName());
            throw new RuntimeException(ex);
        }
    }

    private GremlinExecutor initializeGremlinExecutor(final ExecutorService gremlinExecutorService,
                                                      final ScheduledExecutorService scheduledExecutorService) {
        // initialize graphs from configuration
        if (!graphs.isPresent()) graphs = Optional.of(new Graphs(settings));

        logger.info("Initialized Gremlin thread pool.  Threads in pool named with pattern gremlin-*");

        final GremlinExecutor.Builder gremlinExecutorBuilder = GremlinExecutor.build()
                .scriptEvaluationTimeout(settings.scriptEvaluationTimeout)
                .afterFailure((b, e) -> graphs.get().rollbackAll())
                .afterSuccess(b -> graphs.get().commitAll())
                .beforeEval(b -> graphs.get().rollbackAll())
                .afterTimeout(b -> graphs.get().rollbackAll())
                .enabledPlugins(new HashSet<>(settings.plugins))
                .globalBindings(graphs.get().getGraphsAsBindings())
                .executorService(gremlinExecutorService)
                .scheduledExecutorService(scheduledExecutorService);

        settings.scriptEngines.forEach((k, v) -> gremlinExecutorBuilder.addEngineSettings(k, v.imports, v.staticImports, v.scripts, v.config));
        final GremlinExecutor gremlinExecutor = gremlinExecutorBuilder.create();

        logger.info("Initialized GremlinExecutor and configured ScriptEngines.");

        // script engine init may have altered the graph bindings or maybe even created new ones - need to
        // re-apply those references back
        gremlinExecutor.getGlobalBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof Graph)
                .forEach(kv -> graphs.get().getGraphs().put(kv.getKey(), (Graph) kv.getValue()));

        // script engine init may have constructed the TraversalSource bindings - store them in Graphs object
        gremlinExecutor.getGlobalBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof TraversalSource)
                .forEach(kv -> {
                    logger.info("A {} is now bound to [{}] with {}", kv.getValue().getClass().getSimpleName(), kv.getKey(), kv.getValue());
                    graphs.get().getTraversalSources().put(kv.getKey(), (TraversalSource) kv.getValue());
                });

        return gremlinExecutor;
    }

    /**
     * Stop Gremlin Server and free the port binding. Note that multiple calls to this method will return the
     * same instance of the {@link java.util.concurrent.CompletableFuture}.
     */
    public synchronized CompletableFuture<Void> stop() {
        if (serverStopped != null) {
            // shutdown has started so don't fire it off again
            return serverStopped;
        }

        serverStopped = new CompletableFuture<>();
        final CountDownLatch servicesLeftToShutdown = new CountDownLatch(3);

        ch.close().addListener(f -> servicesLeftToShutdown.countDown());

        logger.info("Shutting down thread pools.");

        try {
            gremlinExecutorService.shutdown();
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
            try {
                gremlinExecutorService.awaitTermination(30000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                logger.warn("Timeout waiting for Gremlin thread pool to shutdown - continuing with shutdown process.");
            }

            try {
                servicesLeftToShutdown.await(30000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                logger.warn("Timeout waiting for bossy/worker thread pools to shutdown - continuing with shutdown process.");
            }

            graphs.ifPresent(gs -> gs.getGraphs().forEach((k, v) -> {
                logger.debug("Closing Graph instance [{}]", k);
                try {
                    v.close();
                } catch (Exception ex) {
                    logger.warn(String.format("Exception while closing Graph instance [%s]", k), ex);
                } finally {
                    logger.info("Closed Graph instance [{}]", k);
                }
            }));

            logger.info("Gremlin Server - shutdown complete");
            serverStopped.complete(null);
        }, SERVER_THREAD_PREFIX + "stop").start();

        return serverStopped;
    }

    public static void main(final String[] args) throws Exception {
        // add to vm options: -Dlog4j.configuration=file:conf/log4j.properties
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
        settings.optionalMetrics().ifPresent(GremlinServer::configureMetrics);
        new GremlinServer(settings).start();
    }

    public static String getHeader() {
        final StringBuilder builder = new StringBuilder();
        builder.append("\r\n");
        builder.append("         \\,,,/\r\n");
        builder.append("         (o o)\r\n");
        builder.append("-----oOOo-(3)-oOOo-----\r\n");
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
                            config.optionalAddressingMode(), config.ttl, config.protocol31, config.hostUUID, config.spoof, config.interval);
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
}
