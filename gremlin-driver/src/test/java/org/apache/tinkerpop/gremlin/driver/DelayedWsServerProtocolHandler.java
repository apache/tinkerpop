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
package org.apache.tinkerpop.gremlin.driver;


import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;

import java.util.List;

class DelayedWsServerProtocolHandler extends WebSocketServerProtocolHandler {

    public WebSocketServerHandshaker handshaker;

    public DelayedWsServerProtocolHandler(String websocketPath, String subprotocols, boolean allowExtensions, WebSocketServerHandshaker hs) {
        super(websocketPath, subprotocols, allowExtensions);
        handshaker = hs;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        // Do nothing.
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) throws Exception {
        if (frame instanceof CloseWebSocketFrame) {
            if (handshaker != null) {
                frame.retain();
                ChannelPromise promise = ctx.newPromise();
                handshaker.close(ctx, (CloseWebSocketFrame) frame, promise);
            }
            return;
        }
        out.add(frame.retain());
    }
}
