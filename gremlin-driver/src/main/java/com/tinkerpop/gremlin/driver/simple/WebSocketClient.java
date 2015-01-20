package com.tinkerpop.gremlin.driver.simple;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.handler.WebSocketClientHandler;
import com.tinkerpop.gremlin.driver.handler.WebSocketGremlinRequestEncoder;
import com.tinkerpop.gremlin.driver.handler.WebSocketGremlinResponseDecoder;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.ser.KryoMessageSerializerV1d0;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.io.IOException;
import java.net.URI;
import java.util.function.Consumer;

/**
 * A simple, non-thread safe Gremlin Server client using websockets.  Typical use is for testing and demonstration.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WebSocketClient implements SimpleClient {
    private final Channel channel;

    private final EventLoopGroup group;
    private final CallbackResponseHandler callbackResponseHandler = new CallbackResponseHandler();

    public WebSocketClient() {
        this(URI.create("ws://localhost:8182"));
    }

    public WebSocketClient(final URI uri) {
        final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("ws-client-%d").build();
        group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(), threadFactory);
        final Bootstrap b = new Bootstrap().group(group);

        final String protocol = uri.getScheme();
        if (!"ws".equals(protocol))
            throw new IllegalArgumentException("Unsupported protocol: " + protocol);

        try {
            final WebSocketClientHandler wsHandler =
                    new WebSocketClientHandler(
                            WebSocketClientHandshakerFactory.newHandshaker(
                                    uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders()));
            final MessageSerializer serializer = new KryoMessageSerializerV1d0();
            b.channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) {
                            final ChannelPipeline p = ch.pipeline();
                            p.addLast(
                                    new HttpClientCodec(),
                                    new HttpObjectAggregator(8192),
                                    wsHandler,
                                    new WebSocketGremlinRequestEncoder(true, serializer),
                                    new WebSocketGremlinResponseDecoder(serializer),
                                    callbackResponseHandler);
                        }
                    });

            channel = b.connect(uri.getHost(), uri.getPort()).sync().channel();
            wsHandler.handshakeFuture().sync();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void submit(final RequestMessage requestMessage, final Consumer<ResponseMessage> callback) throws Exception {
        callbackResponseHandler.callback = callback;
        channel.writeAndFlush(requestMessage).get();
    }

    @Override
    public void close() throws IOException {
        try {
            channel.close().get();
        } catch (Exception ignored) {

        } finally {
            group.shutdownGracefully().awaitUninterruptibly();
        }
    }

    static class CallbackResponseHandler extends SimpleChannelInboundHandler<ResponseMessage> {
        public Consumer<ResponseMessage> callback;

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
            try {
                callback.accept(response);
            } finally {
                ReferenceCountUtil.release(response);
            }
        }
    }
}
