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
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;


/**
 *  An authorization handler for the http channel that allows the {@link Authorizer} to be plugged into it.
 *
 * @author Marc de Lignie
 */
@ChannelHandler.Sharable
public class HttpBasicAuthorizationHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(HttpBasicAuthorizationHandler.class);
    private static final Logger auditLogger = LoggerFactory.getLogger(GremlinServer.AUDIT_LOGGER_NAME);

    private AuthenticatedUser user;
    private final Authorizer authorizer;

    public HttpBasicAuthorizationHandler(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (msg instanceof FullHttpMessage){
            final FullHttpMessage request = (FullHttpMessage) msg;
            final boolean keepAlive = HttpUtil.isKeepAlive(request);

            try {
                user = ctx.channel().attr(StateKey.AUTHENTICATED_USER).get();
                if (null == user) {    // This is expected when using the AllowAllAuthenticator
                    user = AuthenticatedUser.ANONYMOUS_USER;
                }
                final RequestMessage requestMessage = HttpHandlerUtil.getRequestMessageFromHttpRequest((FullHttpRequest) request);
                authorizer.authorize(user, requestMessage);
                ctx.fireChannelRead(request);
            } catch (AuthorizationException ex) {  // Expected: users can alternate between allowed and disallowed requests
                String address = ctx.channel().remoteAddress().toString();
                if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
                final String script;
                try {
                    script = HttpHandlerUtil.getRequestMessageFromHttpRequest((FullHttpRequest) request).getArgOrDefault(Tokens.ARGS_GREMLIN, "");
                } catch (IllegalArgumentException iae) {
                    HttpHandlerUtil.sendError(ctx, BAD_REQUEST, iae.getMessage(), keepAlive);
                    return;
                }
                auditLogger.info("User {} with address {} attempted an unauthorized http request: {}",
                    user.getName(), address, script);
                final String message = String.format("No authorization for script [%s] - check permissions.", script);
                HttpHandlerUtil.sendError(ctx, UNAUTHORIZED, message, keepAlive);
                ReferenceCountUtil.release(msg);
            } catch (Exception ex) {
                final String message = String.format(
                        "%s is not ready to handle requests - unknown error", authorizer.getClass().getSimpleName());
                HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, message, keepAlive);
                ReferenceCountUtil.release(msg);
            }
        } else {
            logger.warn("{} only processes FullHttpMessage instances - received {} - channel closing",
                this.getClass().getSimpleName(), msg.getClass());
            ctx.close();
        }
    }
}
