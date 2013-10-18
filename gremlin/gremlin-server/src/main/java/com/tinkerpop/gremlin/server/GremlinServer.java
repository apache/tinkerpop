package com.tinkerpop.gremlin.server;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Adapted from https://github.com/netty/netty/tree/netty-4.0.10.Final/example/src/main/java/io/netty/example/http/websocketx/server
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServer {
    private static final Logger logger = LoggerFactory.getLogger(GremlinServer.class);
    private final Settings settings;
    private static Optional<Graphs> graphs = Optional.empty();

    public GremlinServer(final Settings settings) {
        this.settings = settings;
    }

    public void run() throws Exception {
        final EventLoopGroup bossGroup = new NioEventLoopGroup();
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            final ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new WebSocketServerInitializer(this.settings));

            final Channel ch = b.bind(settings.host, settings.port).sync().channel();
            logger.info("Web socket server started at port {}.", settings.port);
            logger.info("Open your browser and navigate to http://localhost:{}/", settings.port);

            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
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
            new GremlinServer(settings.get()).run();
        } else
            logger.error("Configuration file at {} could not be found or parsed properly.", file);
    }

    public static String getVersion() {
        return "3.0.0-SNAPSHOT";  // hard-coded for now until tokens available
    }

    public static String getHeader() {
        StringBuilder builder = new StringBuilder();
        builder.append("\r\n");
        builder.append("         \\,,,/\r\n");
        builder.append("         (o o)\r\n");
        builder.append("-----oOOo-(_)-oOOo-----\r\n");
        return builder.toString();
    }

    private static void printHeader() {
        logger.info(getHeader());
    }

    private class WebSocketServerInitializer extends ChannelInitializer<SocketChannel> {
        private final Settings settings;

        public WebSocketServerInitializer(final Settings settings) {
            this.settings = settings;
            synchronized (this) {
                if (!graphs.isPresent()) graphs = Optional.of(new Graphs(settings));
            }
        }

        @Override
        public void initChannel(final SocketChannel ch) throws Exception {
            final ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("codec-http", new HttpServerCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
            pipeline.addLast("handler", new GremlinServerHandler(settings, graphs.get()));
        }
    }

    /**
     * Holder of Graph instances configured for the server to be passed to sessionless bindings.
     */
    public static class Graphs {
        private final Map<String, Graph> graphs;

        public Graphs(final Settings settings) {
            final Map<String, Graph> m = new HashMap<>();
            settings.graphs.entrySet().forEach(e -> {
                try {
                    final Graph newGraph = GraphFactory.open(e.getValue());
                    m.put(e.getKey(), newGraph);
                    logger.info("Graph [{}] was successfully configured via [{}].", e.getKey(), e.getValue());
                } catch (RuntimeException re) {
                    logger.warn("Graph [{}] configured at [{}] could not be instantiated and will not be available in Gremlin Server.  GraphFactory message: {}",
                            e.getKey(), e.getValue(), re.getMessage());
                    if (logger.isDebugEnabled() && re.getCause() != null) logger.debug("GraphFactory exception", re.getCause());
                }
            });
            graphs = Collections.unmodifiableMap(m);
        }

        public Map<String, Graph> getGraphs() {
            return graphs;
        }

        public void rollbackAll() {
            graphs.entrySet().forEach(e->e.getValue().rollback());
        }

        public void commitAll() {
            graphs.entrySet().forEach(e->e.getValue().commit());
        }
    }
}
