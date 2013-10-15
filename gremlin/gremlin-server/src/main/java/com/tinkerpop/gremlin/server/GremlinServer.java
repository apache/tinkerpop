package com.tinkerpop.gremlin.server;

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

import java.util.Optional;

/**
 * Adapted from https://github.com/netty/netty/tree/netty-4.0.10.Final/example/src/main/java/io/netty/example/http/websocketx/server
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServer {
    private static final Logger logger = LoggerFactory.getLogger(GremlinServer.class);
    private final ServerSettings settings;

    public GremlinServer(final ServerSettings settings) {
        this.settings = settings;
    }

    public void run() throws Exception {
        final EventLoopGroup bossGroup = new NioEventLoopGroup();
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            final ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new WebSocketServerInitializer());

            final Channel ch = b.bind(settings.port).sync().channel();
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

        final Optional<ServerSettings> settings = ServerSettings.read(file);
        if (settings.isPresent()) {
            logger.info("Configuring Gremlin Server from {}", file);
            new GremlinServer(settings.get()).run();
        } else
            logger.error("Configuration file at {} could not be found or parsed properly.", file);
    }

    private static void printHeader() {
        logger.info("");
        logger.info("         \\,,,/");
        logger.info("         (o o)");
        logger.info("-----oOOo-(_)-oOOo-----");
    }

    private class WebSocketServerInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        public void initChannel(final SocketChannel ch) throws Exception {
            final ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("codec-http", new HttpServerCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
            pipeline.addLast("handler", new GremlinServerHandler());
        }
    }
}
