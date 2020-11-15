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
package org.apache.tinkerpop.gremlin.server.channel;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketDecoderConfig;
import org.apache.tinkerpop.gremlin.server.AbstractChannelizer;
import org.apache.tinkerpop.gremlin.server.Channelizer;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.server.handler.AbstractAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.WebSocketAuthorizationHandler;
import org.apache.tinkerpop.gremlin.server.handler.SaslAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.WsGremlinBinaryRequestDecoder;
import org.apache.tinkerpop.gremlin.server.handler.WsGremlinCloseRequestDecoder;
import org.apache.tinkerpop.gremlin.server.handler.GremlinResponseFrameEncoder;
import org.apache.tinkerpop.gremlin.server.handler.WsGremlinResponseFrameEncoder;
import org.apache.tinkerpop.gremlin.server.handler.WsGremlinTextRequestDecoder;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Channelizer} that exposes a WebSocket-based Gremlin endpoint with a custom sub-protocol.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WebSocketChannelizer extends AbstractChannelizer {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketChannelizer.class);

    private GremlinResponseFrameEncoder gremlinResponseFrameEncoder;
    private WsGremlinTextRequestDecoder wsGremlinTextRequestDecoder;
    private WsGremlinBinaryRequestDecoder wsGremlinBinaryRequestDecoder;
    private WsGremlinResponseFrameEncoder wsGremlinResponseFrameEncoder;
    private WsGremlinCloseRequestDecoder wsGremlinCloseRequestDecoder;
    private AbstractAuthenticationHandler authenticationHandler;
    private ChannelInboundHandlerAdapter authorizationHandler;

    @Override
    public void init(final ServerGremlinExecutor serverGremlinExecutor) {
        super.init(serverGremlinExecutor);

        gremlinResponseFrameEncoder = new GremlinResponseFrameEncoder();
        wsGremlinTextRequestDecoder = new WsGremlinTextRequestDecoder(serializers);
        wsGremlinBinaryRequestDecoder = new WsGremlinBinaryRequestDecoder(serializers);
        wsGremlinCloseRequestDecoder = new WsGremlinCloseRequestDecoder(serializers);
        wsGremlinResponseFrameEncoder = new WsGremlinResponseFrameEncoder();

        // configure authentication - null means don't bother to add authentication to the pipeline
        authenticationHandler = authenticator.getClass() == AllowAllAuthenticator.class ?
            null : instantiateAuthenticationHandler(settings);
        if (authorizer != null) {
            authorizationHandler = new WebSocketAuthorizationHandler(authorizer);
        }
    }

    @Override
    public void configure(final ChannelPipeline pipeline) {
        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-io", LogLevel.DEBUG));

        logger.debug("HttpRequestDecoder settings - maxInitialLineLength={}, maxHeaderSize={}, maxChunkSize={}",
                settings.maxInitialLineLength, settings.maxHeaderSize, settings.maxChunkSize);
        pipeline.addLast(PIPELINE_HTTP_REQUEST_DECODER, new HttpRequestDecoder(settings.maxInitialLineLength, settings.maxHeaderSize, settings.maxChunkSize));

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-decoder-aggregator", LogLevel.DEBUG));

        logger.debug("HttpObjectAggregator settings - maxContentLength={}, maxAccumulationBufferComponents={}",
                settings.maxContentLength, settings.maxAccumulationBufferComponents);
        final HttpObjectAggregator aggregator = new HttpObjectAggregator(settings.maxContentLength);
        aggregator.setMaxCumulationBufferComponents(settings.maxAccumulationBufferComponents);
        pipeline.addLast("aggregator", aggregator);

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

        pipeline.addLast(PIPELINE_HTTP_RESPONSE_ENCODER, new HttpResponseEncoder());
        // Add compression extension for WebSocket defined in https://tools.ietf.org/html/rfc7692
        pipeline.addLast(PIPELINE_WEBSOCKET_SERVER_COMPRESSION, new WebSocketServerCompressionHandler());

        // setting closeOnProtocolViolation to false prevents causing all the other requests using the same channel
        // to fail when a single request causes a protocol violation.
        final WebSocketDecoderConfig wsDecoderConfig = WebSocketDecoderConfig.newBuilder().
                closeOnProtocolViolation(false).allowExtensions(true).maxFramePayloadLength(settings.maxContentLength).build();
        pipeline.addLast(PIPELINE_REQUEST_HANDLER, new WebSocketServerProtocolHandler(GREMLIN_ENDPOINT,
                null, false, false, 10000L, wsDecoderConfig));

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

        pipeline.addLast("ws-frame-encoder", wsGremlinResponseFrameEncoder);
        pipeline.addLast("response-frame-encoder", gremlinResponseFrameEncoder);
        pipeline.addLast("request-text-decoder", wsGremlinTextRequestDecoder);
        pipeline.addLast("request-binary-decoder", wsGremlinBinaryRequestDecoder);
        pipeline.addLast("request-close-decoder", wsGremlinCloseRequestDecoder);

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

        if (authenticationHandler != null)
            pipeline.addLast(PIPELINE_AUTHENTICATOR, authenticationHandler);

        if (authorizationHandler != null)
            pipeline.addLast(PIPELINE_AUTHORIZER, authorizationHandler);
    }

    @Override
    public boolean supportsIdleMonitor() {
        return true;
    }

    @Override
    public Object createIdleDetectionMessage() {
        return new PingWebSocketFrame();
    }

    private AbstractAuthenticationHandler instantiateAuthenticationHandler(final Settings settings) {
        final String authenticationHandler = settings.authentication.authenticationHandler;
        if (authenticationHandler == null) {
            //Keep things backwards compatible
            return new SaslAuthenticationHandler(authenticator, settings);
        } else {
            return createAuthenticationHandler(settings);
        }
    }
}
