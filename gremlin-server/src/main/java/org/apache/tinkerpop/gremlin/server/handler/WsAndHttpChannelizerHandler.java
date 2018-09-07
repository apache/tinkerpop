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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import org.apache.tinkerpop.gremlin.server.Channelizer;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;

import static org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer.PIPELINE_AUTHENTICATOR;
import static org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer.PIPELINE_REQUEST_HANDLER;
import static org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer.PIPELINE_HTTP_RESPONSE_ENCODER;

/**
 * A ChannelInboundHandlerAdapter for use with {@link WsAndHttpChannelizer} that toggles between WebSockets
 * and http.
 *
 * @author Keith Lohnes lohnesk@gmail.com
 */
@ChannelHandler.Sharable
public class WsAndHttpChannelizerHandler extends ChannelInboundHandlerAdapter {

    private final WebSocketChannelizer wsChannelizer = new WebSocketChannelizer();
    private HttpGremlinEndpointHandler httpGremlinEndpointHandler;

    public void init(final ServerGremlinExecutor serverGremlinExecutor, final HttpGremlinEndpointHandler httpGremlinEndpointHandler) {
        //WebSocketChannelizer has everything needed for the http endpoint to work
        wsChannelizer.init(serverGremlinExecutor);
        this.httpGremlinEndpointHandler = httpGremlinEndpointHandler;
    }

    public Channelizer getWsChannelizer() {
        return wsChannelizer;
    }

    public void configure(final ChannelPipeline pipeline) {
        wsChannelizer.configure(pipeline);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object obj) {
        final ChannelPipeline pipeline = ctx.pipeline();
        if (obj instanceof HttpMessage && !WebSocketHandlerUtil.isWebSocket((HttpMessage)obj)) {
            if (null != pipeline.get(PIPELINE_AUTHENTICATOR)) {
                pipeline.remove(PIPELINE_REQUEST_HANDLER);
                final ChannelHandler authenticator = pipeline.get(PIPELINE_AUTHENTICATOR);
                pipeline.remove(PIPELINE_AUTHENTICATOR);
                pipeline.addAfter(PIPELINE_HTTP_RESPONSE_ENCODER, PIPELINE_AUTHENTICATOR, authenticator);
                pipeline.addAfter(PIPELINE_AUTHENTICATOR, PIPELINE_REQUEST_HANDLER, this.httpGremlinEndpointHandler);
            } else {
                pipeline.remove(PIPELINE_REQUEST_HANDLER);
                pipeline.addAfter(PIPELINE_HTTP_RESPONSE_ENCODER, PIPELINE_REQUEST_HANDLER, this.httpGremlinEndpointHandler);
            }
        }
        ctx.fireChannelRead(obj);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        ctx.close();
    }


}
