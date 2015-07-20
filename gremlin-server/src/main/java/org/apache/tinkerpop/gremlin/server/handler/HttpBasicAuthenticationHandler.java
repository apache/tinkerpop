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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HttpBasicAuthenticationHandler extends ChannelInboundHandlerAdapter {
    private final Authenticator authenticator;

    private final Base64.Decoder decoder = Base64.getUrlDecoder();

    public HttpBasicAuthenticationHandler(final Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (msg instanceof FullHttpMessage) {
            final FullHttpMessage request = (FullHttpMessage) msg;
            if (!request.headers().contains("Authorization")) {
                ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, UNAUTHORIZED));
                ReferenceCountUtil.release(msg);
                return;
            }

            final String authorizationHeader = request.headers().get("Authorization");
            final String authorization = new String(decoder.decode(authorizationHeader), Charset.forName("UTF-8"));
            final String[] split = authorization.split(":");
            if (split.length != 2) {
                ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, UNAUTHORIZED));
                ReferenceCountUtil.release(msg);
                return;
            }

            final Map<String,String> credentials = new HashMap<>();
            credentials.put(PROPERTY_USERNAME, split[0]);
            credentials.put(PROPERTY_PASSWORD, split[1]);

            try {
                authenticator.authenticate(credentials);
                ctx.fireChannelRead(request);
            } catch (AuthenticationException ae) {
                ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, UNAUTHORIZED));
                ReferenceCountUtil.release(msg);
            }
        }
    }
}
