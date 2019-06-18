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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.util.AttributeKey;

/**
 * This handler ensures that WebSocket connections are closed gracefully on the remote server
 * on close of a {@link Channel} by sending a CloseWebSocketFrame to the server.
 * <p>
 * This handler is also idempotent and sends out the CloseFrame only once.
 */
public class WebsocketCloseHandler extends ChannelOutboundHandlerAdapter {
    private static final AttributeKey<Boolean> CLOSE_WS_SENT = AttributeKey.newInstance("closeWebSocketSent");

    @Override
    public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) {
        if (ctx.channel().isActive() && ctx.channel().attr(CLOSE_WS_SENT).compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
            ctx.channel().writeAndFlush(new CloseWebSocketFrame(1000, "Client is closing the channel."), promise)
               .addListener(ChannelFutureListener.CLOSE);
            ctx.pipeline().remove(this);
        }
    }



    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        ctx.channel().attr(CLOSE_WS_SENT).setIfAbsent(Boolean.FALSE);
        super.handlerAdded(ctx);
    }
}
