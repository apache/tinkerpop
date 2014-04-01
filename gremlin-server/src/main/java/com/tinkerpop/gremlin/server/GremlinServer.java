package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Start and stop Gremlin Server.  Adapted from
 * https://github.com/netty/netty/tree/netty-4.0.10.Final/example/src/main/java/io/netty/example/http/websocketx/server
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServer {
    private static final Logger logger = LoggerFactory.getLogger(GremlinServer.class);
    private final Settings settings;
    private Optional<Graphs> graphs = Optional.empty();
    private Channel ch;

    public GremlinServer(final Settings settings) {
        this.settings = settings;
    }

    /**
     * Start Gremlin Server with {@link Settings} provided to the constructor.
     */
    public void run() throws Exception {
        final EventLoopGroup bossGroup = new NioEventLoopGroup(settings.threadPoolBoss);
        final EventLoopGroup workerGroup = new NioEventLoopGroup(settings.threadPoolWorker);
        try {
            final ServerBootstrap b = new ServerBootstrap();

            // when high value is reached then the channel becomes non-writeable and stays like that until the
            // low value is so that there is time to recover
            b.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
            b.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);

            // todo: check again in 4.0.18
            // b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new WebSocketServerInitializer(this.settings));

            ch = b.bind(settings.host, settings.port).sync().channel();
            logger.info("Gremlin Server configured with worker thread pool of {} and boss thread pool of {}",
                    settings.threadPoolWorker, settings.threadPoolBoss);
            logger.info("Websocket channel started at port {}.", settings.port);

            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    /**
     * Stop Gremlin Server and free the port.
     */
    public void stop() {
        ch.close();
    }

    public static void main(final String[] args) throws Exception {
        // add to vm options: -Dlog4j.configuration=file:config/log4j.properties
        printHeader();
        final String file;
        if (args.length > 0)
            file = args[0];
        else
            file = "config/gremlin-server.yaml";

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
        builder.append("-----oOOo-(_)-oOOo-----\r\n");
        return builder.toString();
    }

    private static void configureMetrics(final Settings.ServerMetrics settings) {
        final MetricManager metrics = MetricManager.INSTANCE;
        settings.optionalConsoleReporter().ifPresent(config -> metrics.addConsoleReporter(config.interval));
        settings.optionalCsvReporter().ifPresent(config -> metrics.addCsvReporter(config.interval, config.fileName));
        settings.optionalJmxReporter().ifPresent(config -> metrics.addJmxReporter(config.domain, config.agentId));
        settings.optionalSlf4jReporter().ifPresent(config -> metrics.addSlf4jReporter(config.interval, config.loggerName));
        settings.optionalGangliaReporter().ifPresent(config -> {
            try {
                metrics.addGangliaReporter(config.host, config.port,
                        config.optionalAddressingMode(), config.ttl, config.protocol31, config.hostUUID, config.spoof, config.interval);
            } catch (IOException ioe) {
                logger.warn("Error configuring the Ganglia Reporter.", ioe);
            }
        });
        settings.optionalGraphiteReporter().ifPresent(config -> metrics.addGraphiteReporter(config.host, config.port, config.prefix, config.interval));
    }

    private static void printHeader() {
        logger.info(getHeader());
    }

    private class WebSocketServerInitializer extends ChannelInitializer<SocketChannel> {
        private final Settings settings;
        private final GremlinExecutor gremlinExecutor;

        final EventExecutorGroup gremlinGroup;

        public WebSocketServerInitializer(final Settings settings) {
            this.settings = settings;
            synchronized (this) {
                if (!graphs.isPresent()) graphs = Optional.of(new Graphs(settings));
                gremlinExecutor = new GremlinExecutor(this.settings);
            }

            this.gremlinGroup = new DefaultEventExecutorGroup(settings.gremlinPool, r -> new Thread(r, "gremlin-handler"));

            logger.info("Initialize GremlinExecutor and configured ScriptEngines.");
        }

        @Override
        public void initChannel(final SocketChannel ch) throws Exception {
            final ChannelPipeline pipeline = ch.pipeline();

            if (logger.isDebugEnabled())
                pipeline.addLast(new LoggingHandler("log-io", LogLevel.DEBUG));

            logger.debug("HttpRequestDecoder settings - maxInitialLineLength={}, maxHeaderSize={}, maxChunkSize={}",
                    settings.maxInitialLineLength, settings.maxHeaderSize, settings.maxChunkSize);
            pipeline.addLast("http-request-decoder", new HttpRequestDecoder(settings.maxInitialLineLength, settings.maxHeaderSize, settings.maxChunkSize));

            if (logger.isDebugEnabled())
                pipeline.addLast(new LoggingHandler("log-decoder-aggregator", LogLevel.DEBUG));


            logger.debug("HttpObjectAggregator settings - maxContentLength={}, maxAccumulationBufferComponents={}",
                    settings.maxContentLength, settings.maxAccumulationBufferComponents);
            final HttpObjectAggregator aggregator = new HttpObjectAggregator(settings.maxContentLength);
            aggregator.setMaxCumulationBufferComponents(settings.maxAccumulationBufferComponents);
            pipeline.addLast("aggregator", aggregator);

            if (logger.isDebugEnabled())
                pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

            pipeline.addLast("http-response-encoder", new HttpResponseEncoder());
            pipeline.addLast("request-handler", new WebSocketServerProtocolHandler("/gremlin"));

            if (logger.isDebugEnabled())
                pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

            // todo: configurable encoder in the gremlinGroup??
            pipeline.addLast("response-encoder", new GremlinResponseEncoder(settings, graphs.get(), gremlinExecutor));
            pipeline.addLast("request-decoder", new GremlinRequestDecoder());

            if (logger.isDebugEnabled())
                pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

            pipeline.addLast("op-selector", new OpSelectorHandler(settings, graphs.get(), gremlinExecutor));

            pipeline.addLast(gremlinGroup, "result-iterator-handler", new IteratorHandler(settings, graphs.get(), gremlinExecutor));

            pipeline.addLast(gremlinGroup, "op-executor", new OpExecutorHandler(settings, graphs.get(), gremlinExecutor));
        }
    }
}
