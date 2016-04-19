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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Attribute;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Base64;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.channel.NioChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SASL authentication handler that allows the {@link Authenticator} to be plugged into it. This handler is meant
 * to be used with protocols that process a {@link RequestMessage} such as the {@link WebSocketChannelizer}
 * or the {@link NioChannelizer}
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public class SaslAuthenticationHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(SaslAuthenticationHandler.class);

    private final Authenticator authenticator;

    public SaslAuthenticationHandler(final Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (msg instanceof RequestMessage){
            final RequestMessage requestMessage = (RequestMessage) msg;

            final Attribute<Authenticator.SaslNegotiator> negotiator = ctx.attr(StateKey.NEGOTIATOR);
            final Attribute<RequestMessage> request = ctx.attr(StateKey.REQUEST_MESSAGE);
            if (negotiator.get() == null) {
                // First time through so save the request and send an AUTHENTICATE challenge with no data
                negotiator.set(authenticator.newSaslNegotiator(getRemoteInetAddress(ctx)));
                request.set(requestMessage);
                final ResponseMessage authenticate = ResponseMessage.build(requestMessage)
                        .code(ResponseStatusCode.AUTHENTICATE).create();
                ctx.writeAndFlush(authenticate);
            } else {
                if (requestMessage.getOp().equals(Tokens.OPS_AUTHENTICATION) && requestMessage.getArgs().containsKey(Tokens.ARGS_SASL)) {
                    
                    final Object saslObject = requestMessage.getArgs().get(Tokens.ARGS_SASL);
                    final byte[] saslResponse;
                    
                    if (saslObject instanceof byte[]) {
                        saslResponse = (byte[]) saslObject;
                    } else if(saslObject instanceof String) {
                        saslResponse = Base64.getDecoder().decode((String) saslObject);
                    } else {
                        final ResponseMessage error = ResponseMessage.build(request.get())
                                .statusMessage("Incorrect type for : " + Tokens.ARGS_SASL + ". byte[] or String is expected")
                                .code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).create();
                        ctx.writeAndFlush(error);
                        return;
                    }
                    
                    try {
                        final byte[] saslMessage = negotiator.get().evaluateResponse(saslResponse);
                        if (negotiator.get().isComplete()) {
                            // todo: do something with this user
                            final AuthenticatedUser user = negotiator.get().getAuthenticatedUser();

                            // If we have got here we are authenticated so remove the handler and pass
                            // the original message down the pipeline for processing
                            ctx.pipeline().remove(this);
                            final RequestMessage original = request.get();
                            ctx.fireChannelRead(original);
                        } else {
                            // not done here - send back the sasl message for next challenge
                            final ResponseMessage authenticate = ResponseMessage.build(requestMessage)
                                    .code(ResponseStatusCode.AUTHENTICATE).result(saslMessage).create();
                            ctx.writeAndFlush(authenticate);
                        }
                    } catch (AuthenticationException ae) {
                        final ResponseMessage error = ResponseMessage.build(request.get())
                                .statusMessage(ae.getMessage())
                                .code(ResponseStatusCode.UNAUTHORIZED).create();
                        ctx.writeAndFlush(error);
                    }
                } else {
                    final ResponseMessage error = ResponseMessage.build(requestMessage)
                            .statusMessage("Failed to authenticate")
                            .code(ResponseStatusCode.UNAUTHORIZED).create();
                    ctx.writeAndFlush(error);
                }
            }
        }
        else {
            logger.warn("{} only processes RequestMessage instances - received {} - channel closing",
                    this.getClass().getSimpleName(), msg.getClass());
            ctx.close();
        }
    }

    private InetAddress getRemoteInetAddress(ChannelHandlerContext ctx)
    {
        Channel channel = ctx.channel();

        if (null == channel)
            return null;

        SocketAddress genericSocketAddr = channel.remoteAddress();

        if (null == genericSocketAddr || !(genericSocketAddr instanceof InetSocketAddress))
            return null;

        return ((InetSocketAddress)genericSocketAddr).getAddress();
    }
}
