package com.tinkerpop.gremlin.server.benchmark;

import com.tinkerpop.gremlin.server.MessageSerializer;
import com.tinkerpop.gremlin.server.Tokens;
import com.tinkerpop.gremlin.server.message.RequestMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.util.CharsetUtil;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProfilingApplication {
    public static void main (final String[] args) {

        try {
            final String url = "ws://localhost:8182/gremlin";
            new Worker(20, 100, new URI(url)).execute();


        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

    public static class Worker {
        private final int numberOfConnections;
        private final int numberOfMessages;
        private final URI uri;

        public Worker(final int numberOfConnections, final int numberOfMessages,
                      final URI uri) {
            this.numberOfConnections = numberOfConnections;
            this.numberOfMessages = numberOfMessages;
            this.uri = uri;
        }

        public void execute() throws Exception {
            final EventLoopGroup group = new NioEventLoopGroup();

            final CountDownLatch complete = new CountDownLatch(numberOfConnections);

            for (int ix = 0; ix < numberOfConnections; ix++) {
                final Bootstrap b = new Bootstrap();
                final String protocol = uri.getScheme();
                if (!"ws".equals(protocol)) {
                    throw new IllegalArgumentException("Unsupported protocol: " + protocol);
                }

                final WebSocketClientHandler handler =
                        new WebSocketClientHandler(
                                WebSocketClientHandshakerFactory.newHandshaker(
                                        uri, WebSocketVersion.V13, null, false, HttpHeaders.EMPTY_HEADERS, 1280000));

                b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            final ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast("http-codec", new HttpClientCodec());
                            pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
                            pipeline.addLast("ws-handler", handler);
                        }
                    });

                final Channel ch = b.connect(uri.getHost(), uri.getPort()).sync().channel();
                handler.handshakeFuture().addListener(future -> {
                    CompletableFuture.runAsync(() -> {
                        IntStream.range(0, numberOfMessages).forEach(i -> {
                            final RequestMessage msg = RequestMessage.create(Tokens.OPS_EVAL).add(
                                    Tokens.ARGS_GREMLIN, "1+1", Tokens.ARGS_ACCEPT, "application/json").build();
                            ch.writeAndFlush(new TextWebSocketFrame("application/json|-" + MessageSerializer.DEFAULT_REQUEST_SERIALIZER.serialize(msg)));
                        });

                        ch.close().addListener(f -> complete.countDown());
                    });
                });
            }

            complete.await();
        }
    }

    public static class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private ChannelPromise handshakeFuture;

        public WebSocketClientHandler(final WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        public ChannelFuture handshakeFuture() {
            return handshakeFuture;
        }

        @Override
        public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
            handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) throws Exception {
            handshaker.handshake(ctx.channel());
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
            //System.out.println("WebSocket Client disconnected!");
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final Object msg) throws Exception {
            final Channel ch = ctx.channel();
            if (!handshaker.isHandshakeComplete()) {
                // web socket client connected
                handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                handshakeFuture.setSuccess();
                return;
            }

            if (msg instanceof FullHttpResponse) {
                final FullHttpResponse response = (FullHttpResponse) msg;
                throw new Exception("Unexpected FullHttpResponse (getStatus=" + response.getStatus() + ", content="
                        + response.content().toString(CharsetUtil.UTF_8) + ')');
            }

            final WebSocketFrame frame = (WebSocketFrame) msg;
            if (frame instanceof TextWebSocketFrame) {
                final TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
                System.out.println(textFrame.text());
            } else if (frame instanceof PongWebSocketFrame) {
            } else if (frame instanceof BinaryWebSocketFrame) {
            } else if (frame instanceof CloseWebSocketFrame)
                ch.close();

        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
            cause.printStackTrace();

            if (!handshakeFuture.isDone()) {
                handshakeFuture.setFailure(cause);
            }

            ctx.close();
        }
    }
}
