package com.tinkerpop.gremlin.driver.simple;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.handler.NioGremlinRequestEncoder;
import com.tinkerpop.gremlin.driver.handler.NioGremlinResponseDecoder;
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
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.io.IOException;
import java.net.URI;
import java.util.function.Consumer;

/**
 * A simple, non-thread safe Gremlin Server client using NIO.  Typical use is for testing and demonstration.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NioClient implements SimpleClient {
    private final Channel channel;

    private final EventLoopGroup group;
    private final CallbackResponseHandler callbackResponseHandler = new CallbackResponseHandler();

    public NioClient() {
        this(URI.create("localhost:8182"));
    }

    public NioClient(final URI uri) {
        final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("nio-client-%d").build();
        group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(), threadFactory);
        final Bootstrap b = new Bootstrap().group(group);

        try {
            final MessageSerializer serializer = new KryoMessageSerializerV1d0();
            b.channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) {
                            final ChannelPipeline p = ch.pipeline();
                            p.addLast(
                                    new NioGremlinResponseDecoder(serializer),
                                    new NioGremlinRequestEncoder(true, serializer),
                                    callbackResponseHandler);
                        }
                    });

            channel = b.connect(uri.getHost(), uri.getPort()).sync().channel();
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
