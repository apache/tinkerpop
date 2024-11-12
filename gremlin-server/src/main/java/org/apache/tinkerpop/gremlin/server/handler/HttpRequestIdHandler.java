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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

import java.util.UUID;

/**
 * A handler that generates a Request ID for the incoming HTTP request and injects the same ID into the response header.
 */
@ChannelHandler.Sharable
public class HttpRequestIdHandler extends ChannelDuplexHandler {
    /**
     * The key for whether a {@link io.netty.handler.codec.http.HttpResponse} has been sent for the current response.
     */
    private static final AttributeKey<Boolean> IN_USE = AttributeKey.valueOf("inUse");

    public static String REQUEST_ID_HEADER_NAME = "Gremlin-RequestId";

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            final Boolean currentlyInUse = ctx.channel().attr(IN_USE).get();
            if (currentlyInUse != null && currentlyInUse == true) {
                // Pipelining not supported so just ignore the request if another request already being handled.
                ReferenceCountUtil.release(msg);
                return;
            }

            ctx.channel().attr(StateKey.REQUEST_ID).set(UUID.randomUUID());
            ctx.channel().attr(IN_USE).set(true);
        }

        ctx.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.channel().attr(IN_USE).set(false);
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;
            response.headers().add(REQUEST_ID_HEADER_NAME, ctx.channel().attr(StateKey.REQUEST_ID).get());
        }
        if (msg instanceof LastHttpContent) { // possible for an object to be both HttpResponse and LastHttpContent.
            ctx.channel().attr(IN_USE).set(false);
        }

        ctx.write(msg, promise);
    }
}
