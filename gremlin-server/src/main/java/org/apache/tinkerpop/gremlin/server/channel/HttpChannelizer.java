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
import org.apache.tinkerpop.gremlin.server.AbstractChannelizer;
import org.apache.tinkerpop.gremlin.server.Channelizer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;
import org.apache.tinkerpop.gremlin.server.handler.AbstractAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.HttpBasicAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.HttpBasicAuthorizationHandler;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constructs a {@link Channelizer} that exposes an HTTP endpoint in Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HttpChannelizer extends AbstractChannelizer {
    private static final Logger logger = LoggerFactory.getLogger(HttpChannelizer.class);

    private HttpGremlinEndpointHandler httpGremlinEndpointHandler;

    @Override
    public void init(final ServerGremlinExecutor serverGremlinExecutor) {
        super.init(serverGremlinExecutor);
        httpGremlinEndpointHandler = new HttpGremlinEndpointHandler(serializers, gremlinExecutor, graphManager, settings);
    }

    @Override
    public void configure(final ChannelPipeline pipeline) {
        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-io", LogLevel.DEBUG));

        pipeline.addLast("http-server", new HttpServerCodec());

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("http-io", LogLevel.DEBUG));

        pipeline.addLast(new HttpObjectAggregator(settings.maxContentLength));

        if (authenticator != null) {
            // Cannot add the same handler instance multiple times unless
            // it is marked as @Sharable, indicating a race condition will
            // not occur. It may not be a safe assumption that the handler
            // is sharable so create a new handler each time.
            final AbstractAuthenticationHandler authenticationHandler = authenticator.getClass() == AllowAllAuthenticator.class ?
                    null : instantiateAuthenticationHandler(settings);
            if (authenticationHandler != null)
                pipeline.addLast(PIPELINE_AUTHENTICATOR, authenticationHandler);
        }

        if (authorizer != null) {
            final ChannelInboundHandlerAdapter authorizationHandler = new HttpBasicAuthorizationHandler(authorizer);
            pipeline.addLast(PIPELINE_AUTHORIZER, authorizationHandler);
        }

        pipeline.addLast("http-gremlin-handler", httpGremlinEndpointHandler);
    }

    private AbstractAuthenticationHandler instantiateAuthenticationHandler(final Settings settings) {
        final String authHandlerClass = settings.authentication.authenticationHandler;
        if (authHandlerClass == null) {
            //Keep things backwards compatible
            return new HttpBasicAuthenticationHandler(authenticator, settings);
        } else {
            return createAuthenticationHandler(settings);
        }
    }

    @Override
    public void finalize(final ChannelPipeline pipeline) {
        pipeline.remove(PIPELINE_OP_SELECTOR);
        pipeline.remove(PIPELINE_OP_EXECUTOR);
    }
}
