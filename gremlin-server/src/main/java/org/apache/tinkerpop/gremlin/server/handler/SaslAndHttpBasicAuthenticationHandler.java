/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;

import static org.apache.tinkerpop.gremlin.server.AbstractChannelizer.PIPELINE_AUTHORIZER;
import static org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer.PIPELINE_AUTHENTICATOR;

/**
 * An Authentication Handler for doing WebSocket Sasl and Http Basic auth
 */
@ChannelHandler.Sharable
public class SaslAndHttpBasicAuthenticationHandler extends SaslAuthenticationHandler {

    private final String HTTP_AUTH = "http-authentication";

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #SaslAndHttpBasicAuthenticationHandler(Authenticator, Authorizer, Settings)}.
     */
    @Deprecated
    public SaslAndHttpBasicAuthenticationHandler(final Authenticator authenticator, final Settings settings) {
        this(authenticator, null, settings);
    }

    public SaslAndHttpBasicAuthenticationHandler(final Authenticator authenticator, final Authorizer authorizer, final Settings settings) {
        super(authenticator, authorizer, settings);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void channelRead(final ChannelHandlerContext ctx, final Object obj) throws Exception {
        if (obj instanceof HttpMessage && !WebSocketHandlerUtil.isWebSocket((HttpMessage)obj)) {
            final ChannelPipeline pipeline = ctx.pipeline();
            if (null != pipeline.get(HTTP_AUTH)) {
                pipeline.remove(HTTP_AUTH);
            }
            pipeline.addAfter(PIPELINE_AUTHENTICATOR, HTTP_AUTH, new HttpBasicAuthenticationHandler(authenticator, this.settings));

            if (pipeline.get(PIPELINE_AUTHORIZER) != null) {
                pipeline.remove(PIPELINE_AUTHORIZER);
            }

            // HttpBasicAuthorizationHandler is only added when the pipeline has no HttpGremlinEndpointHandler that
            // authorizes requests itself.
            final HttpGremlinEndpointHandler endpoint = pipeline.get(HttpGremlinEndpointHandler.class);
            if (authorizer != null && (null == endpoint || !endpoint.authorizesRequests())) {
                pipeline.addAfter(HTTP_AUTH, PIPELINE_AUTHORIZER, new HttpBasicAuthorizationHandler(authorizer));
            }

            ctx.fireChannelRead(obj);
        } else {
            super.channelRead(ctx, obj);
        }
    }

}
