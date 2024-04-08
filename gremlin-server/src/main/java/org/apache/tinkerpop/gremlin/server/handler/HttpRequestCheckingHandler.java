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
package org.apache.tinkerpop.gremlin.server.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.ReferenceCountUtil;

import static io.netty.handler.codec.http.HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * A handler that checks for errors in the HTTP request for all routes.
 */
@ChannelHandler.Sharable
public class HttpRequestCheckingHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    public HttpRequestCheckingHandler() {
        // This handler only consumes the FullHttpRequest in certain situations so don't release it.
        super(false);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        // Chunked transfer encoding is used to stream back results so only accept HTTP/1.1 requests.
        if (msg.protocolVersion() != HTTP_1_1) {
            HttpHandlerUtil.sendError(ctx, HTTP_VERSION_NOT_SUPPORTED, "Only HTTP/1.1 is supported.");
            ReferenceCountUtil.release(msg);
            return;
        }

        if ("/favicon.ico".equals(msg.uri())) {
            HttpHandlerUtil.sendError(ctx, NOT_FOUND, "Gremlin Server doesn't have a favicon.ico");
            ReferenceCountUtil.release(msg);
            return;
        }

        ctx.fireChannelRead(msg);
    }
}
