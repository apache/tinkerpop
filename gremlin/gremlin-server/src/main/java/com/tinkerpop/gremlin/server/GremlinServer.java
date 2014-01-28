package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

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
        printHeader();
        final String file;
        if (args.length > 0) {
            file = args[0];
        } else {
            file = "config/gremlin-server.yaml";
        }

        final Optional<Settings> settings = Settings.read(file);
        if (settings.isPresent()) {
            logger.info("Configuring Gremlin Server from {}", file);
            final Settings config = settings.get();
            config.optionalMetrics().ifPresent(GremlinServer::configureMetrics);
            new GremlinServer(config).run();
        } else
            logger.error("Configuration file at {} could not be found or parsed properly.", file);
    }

    public static String getHeader() {
        StringBuilder builder = new StringBuilder();
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

        public WebSocketServerInitializer(final Settings settings) {
            this.settings = settings;
            synchronized (this) {
                if (!graphs.isPresent()) graphs = Optional.of(new Graphs(settings));
                gremlinExecutor = new GremlinExecutor(this.settings);
            }

            logger.info("Initialize GremlinExecutor and configured ScriptEngines.");
        }

        @Override
        public void initChannel(final SocketChannel ch) throws Exception {
            final ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("http-request-decoder", new HttpRequestDecoder());
            pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
            pipeline.addLast("http-response-encoder", new HttpResponseEncoder());
            pipeline.addLast("request-handler", new WebSocketServerProtocolHandler("/gremlin"));
            pipeline.addLast("gremlin-decoder", new GremlinRequestDecoder());

            final EventExecutorGroup gremlinGroup = new DefaultEventExecutorGroup(settings.gremlinPool);
            pipeline.addLast(gremlinGroup, "gremlin-handler", new GremlinServerHandler(settings, graphs.get(), gremlinExecutor));
        }
    }
}
