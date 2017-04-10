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

import io.netty.channel.EventLoopGroup;
import org.apache.tinkerpop.gremlin.server.AbstractChannelizer;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.server.handler.AbstractAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.Settings;
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
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.tinkerpop.gremlin.server.Channelizer} that exposes a WebSocket-based Gremlin endpoint with a custom
 * sub-protocol.
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

    @Override
    public void init(final ServerGremlinExecutor<EventLoopGroup> serverGremlinExecutor) {
        super.init(serverGremlinExecutor);

        gremlinResponseFrameEncoder = new GremlinResponseFrameEncoder();
        wsGremlinTextRequestDecoder = new WsGremlinTextRequestDecoder(serializers);
        wsGremlinBinaryRequestDecoder = new WsGremlinBinaryRequestDecoder(serializers);
        wsGremlinCloseRequestDecoder = new WsGremlinCloseRequestDecoder(serializers);
        wsGremlinResponseFrameEncoder = new WsGremlinResponseFrameEncoder();

        // configure authentication - null means don't bother to add authentication to the pipeline
        if (authenticator != null)
            authenticationHandler = authenticator.getClass() == AllowAllAuthenticator.class ?
                    null : new SaslAuthenticationHandler(authenticator, settings.authentication);
    }

    @Override
    public void configure(final ChannelPipeline pipeline) {
        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-io", LogLevel.DEBUG));

        logger.debug("HttpRequestDecoder settings - maxInitialLineLength={}, maxHeaderSize={}, maxChunkSize={}",
                settings.maxInitialLineLength, settings.maxHeaderSize, settings.maxChunkSize);
        pipeline.addLast("http-request-decoder", new HttpRequestDecoder(settings.maxInitialLineLength, settings.maxHeaderSize, settings.maxChunkSize));

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-decoder-aggregator", LogLevel.DEBUG));

        logger.debug("HttpObjectAggregator settings - maxContentLength={}, maxAccumulationBufferComponents={}",
                settings.maxContentLength, settings.maxAccumulationBufferComponents);
        final HttpObjectAggregator aggregator = new HttpObjectAggregator(settings.maxContentLength);
        aggregator.setMaxCumulationBufferComponents(settings.maxAccumulationBufferComponents);
        pipeline.addLast("aggregator", aggregator);

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

        pipeline.addLast("http-response-encoder", new HttpResponseEncoder());
        pipeline.addLast("request-handler", new WebSocketServerProtocolHandler("/gremlin", null, false, settings.maxContentLength));

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
    }

    private AbstractAuthenticationHandler instantiateAuthenticationHandler(final Settings.AuthenticationSettings authSettings) {
        final String authenticationHandler = authSettings.authenticationHandler;
        if (authenticationHandler == null) {
            //Keep things backwards compatible
            return new SaslAuthenticationHandler(authenticator, authSettings);
        } else {
            return createAuthenticationHandler(authSettings);
        }
    }
}
