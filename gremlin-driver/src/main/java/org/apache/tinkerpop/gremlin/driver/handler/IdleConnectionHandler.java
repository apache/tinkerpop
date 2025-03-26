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
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detects {@link IdleStateEvent}s and closes the channel with the goal of releasing resources that haven't been used in a while.
 */
@ChannelHandler.Sharable
public class IdleConnectionHandler extends ChannelDuplexHandler {
    private static final Logger logger = LoggerFactory.getLogger(IdleConnectionHandler.class);
    public static final AttributeKey<IdleStateEvent> IDLE_STATE_EVENT = AttributeKey.valueOf("idleStateEvent");

    /**
     * Detects if an {@link IdleStateEvent} has occurred and closes the channel.
     */
    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
        if (evt instanceof IdleStateEvent) {
            final IdleStateEvent e = (IdleStateEvent) evt;
            logger.info("Detected IdleStateEvent {} - closing channel {}", e, ctx.channel().id().asShortText());
            ctx.channel().attr(IDLE_STATE_EVENT).set(e);
            ctx.close();
        }
    }
}
