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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;

/**
 * Class that holds the handlers that can used by the initializers in TestChannelizers.
 */
public class TestHandlers {

    /**
     * Handler that will drop requests to the WebSocket path.
     */
    public static class NoOpWebSocketServerHandler extends ChannelInboundHandlerAdapter {
        private String websocketPath;

        public NoOpWebSocketServerHandler(String websocketPath) {
            this.websocketPath = websocketPath;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if ((msg instanceof HttpRequest) && ((HttpRequest) msg).uri().endsWith(websocketPath)) {
                ReferenceCountUtil.release(msg);
            }
        }
    }
}
