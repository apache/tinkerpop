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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Traverser for internal handler classes for constructing the Channel Pipeline.
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
        private static final Logger logger = LoggerFactory.getLogger(GremlinResponseDecoder.class);

        private final MessageSerializer serializer;
        private final ConcurrentMap<UUID, ResponseQueue> pending;

        public GremlinResponseDecoder(final ConcurrentMap<UUID, ResponseQueue> pending, final MessageSerializer serializer) {
            this.pending = pending;
            this.serializer = serializer;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final WebSocketFrame webSocketFrame) throws Exception {
            try {
                if (webSocketFrame instanceof BinaryWebSocketFrame) {
                    final BinaryWebSocketFrame tf = (BinaryWebSocketFrame) webSocketFrame;
                    final ResponseMessage response = serializer.deserializeResponse(tf.content());

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
                } else if (webSocketFrame instanceof TextWebSocketFrame) {
                    final TextWebSocketFrame tf = (TextWebSocketFrame) webSocketFrame;
                    final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                    final ResponseMessage response = textSerializer.deserializeResponse(tf.text());
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
                }
            } finally {
                ReferenceCountUtil.release(webSocketFrame);
            }
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
}
