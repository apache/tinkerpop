package com.tinkerpop.gremlin.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.*;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;

import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final ObjectMapper mapper = new ObjectMapper();

    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    private final WebSocketClient client;

    public WebSocketClientHandler(WebSocketClientHandshaker handshaker, final WebSocketClient client) {
        this.handshaker = handshaker;
        this.client = client;
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        handshaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        //System.out.println("WebSocket Client disconnected!");
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        Channel ch = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            handshaker.finishHandshake(ch, (FullHttpResponse) msg);
            //System.out.println("WebSocket Client connected!");
            handshakeFuture.setSuccess();
            return;
        }

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new Exception("Unexpected FullHttpResponse (getStatus=" + response.getStatus() + ", content="
                    + response.content().toString(CharsetUtil.UTF_8) + ')');
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
            //System.out.println("WebSocket Client received message: " + textFrame.text());
            final JsonNode node = mapper.readTree(textFrame.text());
            this.client.putResponse(UUID.fromString(node.get("requestId").asText()), node);
        } else if (frame instanceof PongWebSocketFrame) {
            //System.out.println("WebSocket Client received pong");
        } else if (frame instanceof CloseWebSocketFrame) {
            //System.out.println("WebSocket Client received closing");
            ch.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();

        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }

        ctx.close();
    }
}
