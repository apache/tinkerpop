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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import org.apache.tinkerpop.gremlin.server.Channelizer;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;

import static org.apache.tinkerpop.gremlin.server.AbstractChannelizer.PIPELINE_HTTP_AGGREGATOR;
import static org.apache.tinkerpop.gremlin.server.AbstractChannelizer.PIPELINE_HTTP_USER_AGENT_HANDLER;
import static org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer.PIPELINE_AUTHENTICATOR;
import static org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer.PIPELINE_REQUEST_HANDLER;

/**
 * A ChannelInboundHandlerAdapter for use with {@link WsAndHttpChannelizer} that toggles between WebSockets
 * and http.
 *
 * @author Keith Lohnes lohnesk@gmail.com
 */
@ChannelHandler.Sharable
public class WsAndHttpChannelizerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(WsAndHttpChannelizerHandler.class);

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
            // If the message is for HTTP and not WS then this handler injects the HTTP user-agent and endpoint handlers
            // in front of the HTTP aggregator to intercept the HttpMessage.
            // This replaces the WS server protocol handler so that the pipeline initially looks like this:
            //
            // IdleStateHandler -> HttpResponseEncoder -> HttpRequestDecoder ->
            //    WsAndHttpChannelizerHandler -> HttpObjectAggregator ->
            //    WebSocketServerCompressionHandler -> WebSocketServerProtocolHandshakeHandler ->
            //    Utf8FrameValidator -> WebSocketServerProtocolHandler -> (more websockets)
            //
            // and shifts to this (setting aside the authentication condition):
            //
            // IdleStateHandler -> HttpResponseEncoder -> HttpRequestDecoder ->
            //    WsAndHttpChannelizerHandler -> HttpObjectAggregator ->
            //    HttpUserAgentHandler -> HttpGremlinEndpointHandler ->
            //    WebSocketServerCompressionHandler -> WebSocketServerProtocolHandshakeHandler ->
            //    Utf8FrameValidator -> (more websockets)
            pipeline.remove(PIPELINE_REQUEST_HANDLER);
            if (null != pipeline.get(PIPELINE_HTTP_USER_AGENT_HANDLER)) {
                pipeline.remove(PIPELINE_HTTP_USER_AGENT_HANDLER);
            }
            if (null != pipeline.get(PIPELINE_AUTHENTICATOR)) {
                final ChannelHandler authenticator = pipeline.get(PIPELINE_AUTHENTICATOR);
                pipeline.remove(PIPELINE_AUTHENTICATOR);
                pipeline.addAfter(PIPELINE_HTTP_AGGREGATOR, PIPELINE_AUTHENTICATOR, authenticator);
                pipeline.addAfter(PIPELINE_AUTHENTICATOR, PIPELINE_HTTP_USER_AGENT_HANDLER, new HttpUserAgentHandler());
                pipeline.addAfter(PIPELINE_HTTP_USER_AGENT_HANDLER, PIPELINE_REQUEST_HANDLER, this.httpGremlinEndpointHandler);
            } else {
                pipeline.addAfter(PIPELINE_HTTP_AGGREGATOR, PIPELINE_HTTP_USER_AGENT_HANDLER, new HttpUserAgentHandler());
                pipeline.addAfter(PIPELINE_HTTP_USER_AGENT_HANDLER, PIPELINE_REQUEST_HANDLER, this.httpGremlinEndpointHandler);
                // Note that channelRead()'s do not propagate down the pipeline past HttpGremlinEndpointHandler
            }
        }
        ctx.fireChannelRead(obj);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        // The log printing here is necessary because when an exception thrown by Netty reaches this method,
        // if the error is not printed, the server side will not be able to perceive what has happened.
        // For example, if the maxContentLength in JanusGraph's Gremlin-Server configuration is set to 65536,
        // and the client sends data over this size via WebSocket, the JanusGraph server logs will appear normal,
        // but the client will experience a disconnection, making it difficult to immediately identify the cause of the issue.
        logger.error("Could not process the request", cause);
        ctx.close();
    }


}
