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
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

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
        if (msg instanceof RequestMessage) {
            final RequestMessage requestMessage = (RequestMessage) msg;

            try {
                user = ctx.channel().attr(StateKey.AUTHENTICATED_USER).get();
                if (null == user) {    // This is expected when using the AllowAllAuthenticator
                    user = AuthenticatedUser.ANONYMOUS_USER;
                }

                final String gremlin = requestMessage.getGremlin();
                final Map<String, String> aliases = new HashMap<>();
                aliases.put(Tokens.ARGS_G, requestMessage.getField(Tokens.ARGS_G));
                final String restrictedGremlin = authorizer.authorize(user, gremlin, aliases);
                final RequestMessage restrictedMsg = RequestMessage.from(requestMessage, restrictedGremlin).create();
                ctx.fireChannelRead(restrictedMsg);

            } catch (AuthorizationException ex) {  // Expected: users can alternate between allowed and disallowed requests
                String address = ctx.channel().remoteAddress().toString();
                if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
                final String script = requestMessage.getGremlin().toString();
                auditLogger.info("User {} with address {} attempted an unauthorized http request: {}",
                    user.getName(), address, script);
                HttpHandlerUtil.sendError(ctx, UNAUTHORIZED, "Failed to authorize: " + ex.getMessage());
                ReferenceCountUtil.release(msg);
            } catch (Exception ex) {
                final String message = String.format(
                        "%s is not ready to handle requests - unknown error", authorizer.getClass().getSimpleName());
                HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, message);
                ReferenceCountUtil.release(msg);
            }
        } else {
            logger.warn("{} only processes FullHttpMessage instances - received {} - channel closing",
                this.getClass().getSimpleName(), msg.getClass());
            ctx.close();
        }
    }
}
