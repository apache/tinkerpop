package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import com.tinkerpop.gremlin.server.util.MetricManager;
import com.tinkerpop.gremlin.structure.Graph;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

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

    private static final Logger logger = LoggerFactory.getLogger(GremlinServer.class);
    private final Settings settings;
    private Optional<Graphs> graphs = Optional.empty();
    private Channel ch;

    private final Optional<CompletableFuture<Void>> serverReady;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final EventExecutorGroup gremlinGroup;

    public GremlinServer(final Settings settings) {
        this(settings, null);
    }

    public GremlinServer(final Settings settings, final CompletableFuture<Void> serverReady) {
        this.serverReady = Optional.ofNullable(serverReady);
        this.settings = settings;

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "gremlin-shutdown-hook"));

        bossGroup = new NioEventLoopGroup(settings.threadPoolBoss);
        workerGroup = new NioEventLoopGroup(settings.threadPoolWorker);

        final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("gremlin-%d").build();
        gremlinGroup = new DefaultEventExecutorGroup(settings.gremlinPool, threadFactory);
    }

    /**
     * Start Gremlin Server with {@link Settings} provided to the constructor.
     */
    public void run() throws Exception {
        try {
            final ServerBootstrap b = new ServerBootstrap();

            // when high value is reached then the channel becomes non-writable and stays like that until the
            // low value is so that there is time to recover
            b.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, settings.writeBufferLowWaterMark);
            b.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, settings.writeBufferHighWaterMark);
            b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            final GremlinExecutor gremlinExecutor = initializeGremlinExecutor(gremlinGroup, workerGroup);
            final Channelizer channelizer = createChannelizer(settings);
            channelizer.init(settings, gremlinExecutor, gremlinGroup, graphs.get(), workerGroup);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(channelizer);

            // bind to host/port and wait for channel to be ready
            ch = b.bind(settings.host, settings.port).sync().channel();

            logger.info("Gremlin Server configured with worker thread pool of {} and boss thread pool of {}",
                    settings.threadPoolWorker, settings.threadPoolBoss);
            logger.info("Channel started at port {}.", settings.port);

            serverReady.ifPresent(future -> future.complete(null));
        } catch (Exception ex) {
            logger.error("Gremlin Server Error", ex);
        }
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

    private GremlinExecutor initializeGremlinExecutor(final EventExecutorGroup gremlinGroup,
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
                .executorService(gremlinGroup)
                .scheduledExecutorService(scheduledExecutorService);

        settings.scriptEngines.forEach((k, v) -> gremlinExecutorBuilder.addEngineSettings(k, v.imports, v.staticImports, v.scripts, v.config));
        final GremlinExecutor gremlinExecutor = gremlinExecutorBuilder.create();

        logger.info("Initialized GremlinExecutor and configured ScriptEngines.");

        // script engine init may have altered the graph bindings or maybe even created new ones - need to
        // re-apply those references back
        gremlinExecutor.getGlobalBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof Graph)
                .forEach(kv -> graphs.get().getGraphs().put(kv.getKey(), (Graph) kv.getValue()));

        return gremlinExecutor;
    }

    /**
     * Stop Gremlin Server and free the port binding.
     */
    public void stop() {
        ch.close();

        logger.info("Shutting down thread pools.");

        try {
            gremlinGroup.shutdownGracefully();
        } finally {
            logger.debug("Shutdown Gremlin thread pool.");
        }

        try {
            workerGroup.shutdownGracefully();
        } finally {
            logger.debug("Shutdown Worker thread pool.");
        }
        try {
            bossGroup.shutdownGracefully();
        } finally {
            logger.debug("Shutdown Boss thread pool.");
        }

        // channel is shutdown as are the thread pools - time to kill graphs as nothing else should be acting on them
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
        new GremlinServer(settings).run();
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
