package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.ser.JsonMessageSerializerV1d0;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Holder for internal handler classes for constructing the Channel Pipeline.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Handler {
    static class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
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

            // todo: properly deal with close() - meaning event handler of some sort for the Connection
            final WebSocketFrame frame = (WebSocketFrame) msg;
            if (frame instanceof TextWebSocketFrame) {
                ctx.fireChannelRead(frame.retain(2));
            } else if (frame instanceof PongWebSocketFrame) {
            } else if (frame instanceof BinaryWebSocketFrame) {
                ctx.fireChannelRead(frame.retain(2));
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

    static class GremlinResponseDecoder extends SimpleChannelInboundHandler<WebSocketFrame> {
        // todo: serializer configuration
        private static MessageSerializer serializer = new JsonMessageSerializerV1d0();
        private final ConcurrentMap<UUID, ResponseQueue> pending;

        public GremlinResponseDecoder(final ConcurrentMap<UUID, ResponseQueue> pending) {
            this.pending = pending;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final WebSocketFrame webSocketFrame) throws Exception {
            try {
                if (webSocketFrame instanceof BinaryWebSocketFrame) {
                    // todo: binary decode this guy
                } else if (webSocketFrame instanceof TextWebSocketFrame) {
                    final TextWebSocketFrame tf = (TextWebSocketFrame) webSocketFrame;
                    final ResponseMessage response = serializer.deserializeResponse(tf.text()).get();
                    if (response.getCode() == ResultCode.SUCCESS)
                        pending.get(response.getRequestId()).add(response);
                    else if (response.getCode() == ResultCode.SUCCESS_TERMINATOR)
                        pending.remove(response.getRequestId()).markComplete();
                    else
                        pending.get(response.getRequestId()).markError(new RuntimeException(response.getResult().toString()));
                }
            } finally {
                ReferenceCountUtil.release(webSocketFrame);
            }
        }
    }

    static class GremlinRequestEncoder extends MessageToMessageEncoder<RequestMessage> {
        private boolean binaryEncoding = false;

        // todo: serializer configuration
        private static MessageSerializer serializer = new JsonMessageSerializerV1d0();

        public GremlinRequestEncoder(final boolean binaryEncoding) {
            this.binaryEncoding = binaryEncoding;
        }

        @Override
        protected void encode(final ChannelHandlerContext channelHandlerContext, final RequestMessage requestMessage, final List<Object> objects) throws Exception {
            if (binaryEncoding) {
                // todo: send BinaryWebSocketFrame
            } else {
                objects.add(new TextWebSocketFrame("application/json|-" + serializer.serializeRequest(requestMessage)));
            }
        }
    }
}
