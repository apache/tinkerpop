package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.exception.ResponseException;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.message.ResultType;
import com.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Traverser for internal handler classes for constructing the Channel Pipeline.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Handler {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    static class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
        private static final Logger logger = LoggerFactory.getLogger(WebSocketClientHandler.class);
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
            logger.warn("Exception caught during WebSocket processing - closing connection", cause);
            if (!handshakeFuture.isDone()) handshakeFuture.setFailure(cause);
            ctx.close();
        }
    }

    static class GremlinResponseHandler extends SimpleChannelInboundHandler<ResponseMessage> {
        private static final Logger logger = LoggerFactory.getLogger(GremlinResponseHandler.class);

        private final ConcurrentMap<UUID, ResponseQueue> pending;

        public GremlinResponseHandler(final ConcurrentMap<UUID, ResponseQueue> pending) {
            this.pending = pending;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
            try {
                if (response.getCode() == ResultCode.SUCCESS) {
                    if (response.getResultType() == ResultType.OBJECT)
                        pending.get(response.getRequestId()).add(response);
                    else if (response.getResultType() == ResultType.COLLECTION) {
                        // unrolls the collection into individual response messages to be handled by the queue
                        final List<Object> listToUnroll = (List<Object>) response.getResult();
                        final ResponseQueue queue = pending.get(response.getRequestId());
                        listToUnroll.forEach(item -> queue.add(
                                ResponseMessage.create(response.getRequestId())
                                        .result(item).build()));
                    } else if (response.getResultType() == ResultType.EMPTY) {
                        // there is nothing to do with ResultType.EMPTY - it will simply be marked complete with
                        // a success terminator
                    } else {
                        logger.warn("Received an invalid ResultType of [{}] - marking request {} as being in error. Please report as this issue.", response.getResultType(), response.getRequestId());
                        pending.get(response.getRequestId()).markError(new RuntimeException(response.getResult().toString()));
                    }
                } else if (response.getCode() == ResultCode.SUCCESS_TERMINATOR)
                    pending.remove(response.getRequestId()).markComplete();
                else
                    pending.get(response.getRequestId()).markError(new ResponseException(response.getCode(), response.getResult().toString()));
            } finally {
                ReferenceCountUtil.release(response);
            }
        }
    }

    static class WebSocketGremlinResponseDecoder extends MessageToMessageDecoder<WebSocketFrame> {
        private final MessageSerializer serializer;

        public WebSocketGremlinResponseDecoder(final MessageSerializer serializer) {
            this.serializer = serializer;
        }

        @Override
        protected void decode(final ChannelHandlerContext channelHandlerContext, final WebSocketFrame webSocketFrame, final List<Object> objects) throws Exception {
            try {
                if (webSocketFrame instanceof BinaryWebSocketFrame) {
                    final BinaryWebSocketFrame tf = (BinaryWebSocketFrame) webSocketFrame;
                    objects.add(serializer.deserializeResponse(tf.content()));
                } else {
                    final TextWebSocketFrame tf = (TextWebSocketFrame) webSocketFrame;
                    final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                    objects.add(textSerializer.deserializeResponse(tf.text()));
                }
            } finally {
                ReferenceCountUtil.release(webSocketFrame);
            }
        }
    }

    static class NioGremlinResponseDecoder extends ByteToMessageDecoder {
        private final MessageSerializer serializer;

        public NioGremlinResponseDecoder(final MessageSerializer serializer) {
            this.serializer = serializer;
        }

        @Override
        protected void decode(final ChannelHandlerContext channelHandlerContext, final ByteBuf byteBuf, final List<Object> objects) throws Exception {
            // todo: won't decode "text"
            if (byteBuf.readableBytes() < 1) {
                return;
            }

            objects.add(serializer.deserializeResponse(byteBuf));
        }
    }

    static class GremlinRequestEncoder extends MessageToMessageEncoder<RequestMessage> {
        private static final Logger logger = LoggerFactory.getLogger(GremlinRequestEncoder.class);
        private boolean binaryEncoding = false;

        private final MessageSerializer serializer;

        public GremlinRequestEncoder(final boolean binaryEncoding, final MessageSerializer serializer) {
            this.binaryEncoding = binaryEncoding;
            this.serializer = serializer;
        }

        @Override
        protected void encode(final ChannelHandlerContext channelHandlerContext, final RequestMessage requestMessage, final List<Object> objects) throws Exception {
            try {
                if (binaryEncoding) {
                    final ByteBuf encodedMessage = serializer.serializeRequestAsBinary(requestMessage, channelHandlerContext.alloc());
                    objects.add(new BinaryWebSocketFrame(encodedMessage));
                } else {
                    final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                    objects.add(new TextWebSocketFrame(textSerializer.serializeRequestAsString(requestMessage)));
                }
            } catch (Exception ex) {
                logger.warn(String.format("An error occurred during serialization of this request [%s] - it could not be sent to the server.", requestMessage), ex);
            }
        }
    }

    static class NioGremlinRequestEncoder extends MessageToByteEncoder<Object> {
        private static final Logger logger = LoggerFactory.getLogger(GremlinRequestEncoder.class);
        private boolean binaryEncoding = false;

        private final MessageSerializer serializer;

        public NioGremlinRequestEncoder(final boolean binaryEncoding, final MessageSerializer serializer) {
            this.binaryEncoding = binaryEncoding;
            this.serializer = serializer;
        }

        @Override
        protected void encode(final ChannelHandlerContext channelHandlerContext, final Object msg, final ByteBuf byteBuf) throws Exception {
            final RequestMessage requestMessage = (RequestMessage) msg;
            try {
                if (binaryEncoding) {
                    byteBuf.writeBytes(serializer.serializeRequestAsBinary(requestMessage, channelHandlerContext.alloc()));
                } else {
                    final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                    byteBuf.writeBytes(textSerializer.serializeRequestAsString(requestMessage).getBytes(UTF8));
                }
            } catch (Exception ex) {
                logger.warn(String.format("An error occurred during serialization of this request [%s] - it could not be sent to the server.", requestMessage), ex);
            }
        }
    }
}
