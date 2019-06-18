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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class WebSocketIdleEventHandler extends ChannelDuplexHandler {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketIdleEventHandler.class);
    private final ChannelGroup activeChannels;

    public WebSocketIdleEventHandler(final ChannelGroup activeChannels) {
        this.activeChannels = activeChannels;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        activeChannels.add(ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) event;
            if (e.state() == IdleState.READER_IDLE) {
                logger.warn("Server " + ctx.channel() + " has been idle for too long.");
            } else if (e.state() == IdleState.WRITER_IDLE || e.state() == IdleState.ALL_IDLE) {
                logger.info("Sending ping frame to the server");
                ctx.writeAndFlush(new PingWebSocketFrame());
            }
        }
    }
}
