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
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper over {@link WebSocketClientProtocolHandler}. This wrapper provides a future which represents the termination
 * of a WS handshake (both success and failure).
 */
public final class WebSocketClientHandler extends WebSocketClientProtocolHandler {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketClientHandler.class);

    private final long handshakeTimeoutMillis;
    private ChannelPromise handshakeFuture;

    public WebSocketClientHandler(final WebSocketClientHandshaker handshaker, final long timeoutMillis) {
        super(handshaker, /*handleCloseFrames*/true, /*dropPongFrames*/true, timeoutMillis);
        this.handshakeTimeoutMillis = timeoutMillis;
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) {
        super.handlerAdded(ctx);
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }

        // let the GremlinResponseHandler take care of exception logging, channel closing, and cleanup
        ctx.fireExceptionCaught(cause);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (!handshakeFuture.isDone()) {
            // channel was closed before the handshake could be completed.
            handshakeFuture.setFailure(
                    new RuntimeException(String.format("Channel=[%s] closed before the handshake could complete",
                            ctx.channel().toString())));
        }

        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) event;
            if (e.state() == IdleState.READER_IDLE) {
                logger.warn("WebSocket connection " + ctx.channel() + " has been idle for too long.");
            } else if (e.state() == IdleState.WRITER_IDLE) {
                logger.debug("Sending ping frame to the server");
                ctx.writeAndFlush(new PingWebSocketFrame());
            }
        } else if (ClientHandshakeStateEvent.HANDSHAKE_COMPLETE.equals(event)) {
            if (!handshakeFuture.isDone()) {
                handshakeFuture.setSuccess();
            }
        } else if (ClientHandshakeStateEvent.HANDSHAKE_TIMEOUT.equals(event)) {
            if (!handshakeFuture.isDone()) {
                handshakeFuture.setFailure(
                        new TimeoutException(String.format("handshake not completed in stipulated time=[%s]ms",
                                handshakeTimeoutMillis)));
            }
        } else {
            super.userEventTriggered(ctx, event);
        }
    }
}
