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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

/**
 * Handler used to detect if the channel has become inactive and throw an error in certain scenarios.
 */
@ChannelHandler.Sharable
public class InactiveChannelHandler extends ChannelInboundHandlerAdapter {
    public static final AttributeKey<Integer> BYTES_READ = AttributeKey.valueOf("bytesRead");
    public static final AttributeKey<Boolean> REQUEST_SENT = AttributeKey.valueOf("requestSent");

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // If the server requires SSL/TLS, then it will simply shut down the connection if it doesn't get the right
        // handshake. This happens after the TCP connection has already been created. If no packets were received from
        // the server and the connection closes after an HTTP request was sent, then there is a good chance that this is
        // because the server expected a SSL/TLS handshake.
        if ((null == ctx.channel().attr(BYTES_READ).get()) && (null != ctx.channel().attr(REQUEST_SENT).get())) {
            // BYTES_READ is set by the HttpGremlinResponseStreamDecoder when any data is received. If it isn't set,
            // it can be assumed that no bytes were received when the connection closed.
            String errMsg;
            if (ctx.channel().hasAttr(IdleConnectionHandler.IDLE_STATE_EVENT)) {
                errMsg = "Idle timeout occurred before response could be received from server - consider increasing idleConnectionTimeout";
            } else {
                errMsg = "Connection to server closed unexpectedly. Ensure that the server is still" +
                        " reachable. The server may be expecting SSL to be enabled.";
            }
            ctx.fireExceptionCaught(new RuntimeException(errMsg));
        } else {
            super.channelInactive(ctx);
        }
    }
}
