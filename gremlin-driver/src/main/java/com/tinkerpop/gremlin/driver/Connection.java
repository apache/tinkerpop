package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A single connection to a Gremlin Server instance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Connection {
    private final Channel channel;
    private final URI uri;
    private final ConcurrentMap<UUID, ResponseQueue> pending = new ConcurrentHashMap<>();

    public Connection(final URI uri, final Cluster.Factory factory)  {
        this.uri = uri;
        final Bootstrap b = factory.createBootstrap();
        final String protocol = uri.getScheme();
        if (!"ws".equals(protocol))
            throw new IllegalArgumentException("Unsupported protocol: " + protocol);

        final ClientPipelineInitializer initializer = new ClientPipelineInitializer();
        b.channel(NioSocketChannel.class).handler(initializer);

        // todo: blocking
        try {
            channel = b.connect(uri.getHost(), uri.getPort()).sync().channel();
            initializer.handler.handshakeFuture().sync();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
            throw new RuntimeException(ie);
        }
    }

    public ChannelPromise write(final RequestMessage requestMessage, final CompletableFuture<ResultSet> future) {
        // once there is a completed write, then create a holder for the result set and complete
        // the promise so that the client knows that that it can start checking for results.
        final ChannelPromise promise = channel.newPromise()
                .addListener(f -> {
                    final LinkedBlockingQueue<ResponseMessage> responseQueue = new LinkedBlockingQueue<>();
                    final ResponseQueue handler = new ResponseQueue(responseQueue);
                    pending.put(requestMessage.getRequestId(), handler);
                    final ResultSet resultSet = new ResultSet(handler);
                    future.complete(resultSet);
                });
        channel.writeAndFlush(requestMessage, promise);
        return promise;
    }

    public void close() throws InterruptedException {
        //System.out.println("WebSocket Client sending close");
        channel.writeAndFlush(new CloseWebSocketFrame());
        channel.closeFuture().sync();
        //group.shutdownGracefully();
    }

    class ClientPipelineInitializer extends ChannelInitializer<SocketChannel> {

        // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
        // If you change it to V00, ping is not supported and remember to change
        // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
        final Handler.WebSocketClientHandler handler;

        public ClientPipelineInitializer() {
            handler = new Handler.WebSocketClientHandler(
                          WebSocketClientHandshakerFactory.newHandshaker(
                              Connection.this.uri, WebSocketVersion.V13, null, false, HttpHeaders.EMPTY_HEADERS, 1280000));
        }

        @Override
        protected void initChannel(final SocketChannel socketChannel) throws Exception {
            final ChannelPipeline pipeline = socketChannel.pipeline();
            pipeline.addLast("http-codec", new HttpClientCodec());
            pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
            pipeline.addLast("ws-handler", handler);
            pipeline.addLast("gremlin-encoder", new Handler.GremlinRequestEncoder(false));
            pipeline.addLast("gremlin-decoder", new Handler.GremlinResponseDecoder(pending));
        }
    }


}
