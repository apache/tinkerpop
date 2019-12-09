/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketClientHandler.class);
    private ChannelPromise handshakeFuture;
    private final ChannelGroup activeChannels;

    public WebSocketClientHandler(final ChannelGroup activeChannels) {
        this.activeChannels = activeChannels;
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        handshakeFuture = ctx.newPromise();
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        final WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof TextWebSocketFrame) {
            ctx.fireChannelRead(frame.retain(2));
        } else if (frame instanceof BinaryWebSocketFrame) {
            ctx.fireChannelRead(frame.retain(2));
        } else if (frame instanceof PongWebSocketFrame) {
            logger.debug("Received response from keep-alive request");
        }
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            final IdleStateEvent e = (IdleStateEvent) event;
            if (e.state() == IdleState.READER_IDLE) {
                logger.warn("Server " + ctx.channel() + " has been idle for too long.");
            } else if (e.state() == IdleState.WRITER_IDLE || e.state() == IdleState.ALL_IDLE) {
                logger.info("Sending ping frame to the server");
                ctx.writeAndFlush(new PingWebSocketFrame());
            }
        } else if (event == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
            handshakeFuture.setSuccess();
            activeChannels.add(ctx.channel());
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        if (!handshakeFuture.isDone()) handshakeFuture.setFailure(cause);

        // let the GremlinResponseHandler take care of exception logging, channel closing, and cleanup
        ctx.fireExceptionCaught(cause);
    }
}
