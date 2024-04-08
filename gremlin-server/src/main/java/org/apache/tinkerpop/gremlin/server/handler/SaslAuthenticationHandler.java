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
import io.netty.util.Attribute;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

/**
 * A SASL authentication handler that allows the {@link Authenticator} to be plugged into it. This handler is meant
 * to be used with protocols that process a {@link RequestMessage} such as the {@link WebSocketChannelizer}
 *
 * @author Stephen Mallette (<a href="http://stephen.genoprime.com">http://stephen.genoprime.com</a>)
 */
@ChannelHandler.Sharable
public class SaslAuthenticationHandler extends AbstractAuthenticationHandler {
    private static final Logger logger = LoggerFactory.getLogger(SaslAuthenticationHandler.class);
    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    public static final Duration MAX_REQUEST_DEFERRABLE_DURATION = Duration.ofSeconds(5);
    private static final Logger auditLogger = LoggerFactory.getLogger(GremlinServer.AUDIT_LOGGER_NAME);

    protected final Settings settings;

    /**
     * @deprecated As of release 3.5.0, replaced by {@link #SaslAuthenticationHandler(Authenticator, Authorizer, Settings)}.
     */
    @Deprecated
    public SaslAuthenticationHandler(final Authenticator authenticator, final Settings settings) {
        this(authenticator, null, settings);
    }

    public SaslAuthenticationHandler(final Authenticator authenticator, final Authorizer authorizer, final Settings settings) {
        super(authenticator, authorizer);
        this.settings = settings;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (!(msg instanceof RequestMessage)) {
            logger.warn("{} only processes RequestMessage instances - received {} - channel closing",
                    this.getClass().getSimpleName(), msg.getClass());
            ctx.close();
            return;
        }

        final RequestMessage requestMessage = (RequestMessage) msg;

        final Attribute<Authenticator.SaslNegotiator> negotiator = ctx.channel().attr(StateKey.NEGOTIATOR);
        final Attribute<RequestMessage> request = ctx.channel().attr(StateKey.REQUEST_MESSAGE);
        final Attribute<Pair<LocalDateTime, List<RequestMessage>>> deferredRequests = ctx.channel().attr(StateKey.DEFERRED_REQUEST_MESSAGES);

        if (negotiator.get() == null) {
            try {
                // First time through so save the request and send an AUTHENTICATE challenge with no data
                negotiator.set(authenticator.newSaslNegotiator(getRemoteInetAddress(ctx)));
                request.set(requestMessage);
                final ResponseMessage authenticate = ResponseMessage.build(requestMessage)
                        .code(ResponseStatusCode.AUTHENTICATE).create();
                ctx.writeAndFlush(authenticate);
            } catch (Exception ex) {
                // newSaslNegotiator can cause troubles - if we don't catch and respond nicely the driver seems
                // to hang until timeout which isn't so nice. treating this like a server error as it means that
                // the Authenticator isn't really ready to deal with requests for some reason.
                logger.error(String.format("%s is not ready to handle requests - check its configuration or related services",
                        authenticator.getClass().getSimpleName()), ex);

                respondWithError(
                        requestMessage,
                        builder -> builder.statusMessage("Authenticator is not ready to handle requests").code(ResponseStatusCode.SERVER_ERROR),
                        ctx);
            }

            return;
        } else if (!requestMessage.getOp().equals(Tokens.OPS_AUTHENTICATION)) {
            // If authentication negotiation is pending, store subsequent non-authentication requests for later processing
            deferredRequests.setIfAbsent(new ImmutablePair<>(LocalDateTime.now(), new ArrayList<>()));
            deferredRequests.get().getValue().add(requestMessage);

            final Duration deferredDuration = Duration.between(deferredRequests.get().getKey(), LocalDateTime.now());

            if (deferredDuration.compareTo(MAX_REQUEST_DEFERRABLE_DURATION) > 0) {
                respondWithError(
                        requestMessage,
                        builder -> builder.statusMessage("Authentication did not finish in the allowed duration (" + MAX_REQUEST_DEFERRABLE_DURATION + "s).")
                                    .code(ResponseStatusCode.UNAUTHORIZED),
                        ctx);
                return;
            }

            return;
        } else if (!requestMessage.getArgs().containsKey(Tokens.ARGS_SASL)) {
            // This is an authentication request that is missing a "sasl" argument.
            respondWithError(
                    requestMessage,
                    builder -> builder.statusMessage("Failed to authenticate").code(ResponseStatusCode.UNAUTHORIZED),
                    ctx);
            return;
        }

        final Object saslObject = requestMessage.getArgs().get(Tokens.ARGS_SASL);

        if (!(saslObject instanceof String)) {
            respondWithError(
                    requestMessage,
                    builder -> builder
                            .statusMessage("Incorrect type for : " + Tokens.ARGS_SASL + " - base64 encoded String is expected")
                            .code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST),
                    ctx);
            return;
        }

        try {
            final byte[] saslResponse = BASE64_DECODER.decode((String) saslObject);
            final byte[] saslMessage = negotiator.get().evaluateResponse(saslResponse);

            if (!negotiator.get().isComplete()) {
                // not done here - send back the sasl message for next challenge.
                final HashMap<String, Object> metadata = new HashMap<>();
                metadata.put(Tokens.ARGS_SASL, BASE64_ENCODER.encodeToString(saslMessage));
                final ResponseMessage authenticate = ResponseMessage.build(requestMessage)
                        .statusAttributes(metadata)
                        .code(ResponseStatusCode.AUTHENTICATE).create();
                ctx.writeAndFlush(authenticate);
                return;
            }

            final org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser user = negotiator.get().getAuthenticatedUser();
            ctx.channel().attr(StateKey.AUTHENTICATED_USER).set(user);
            // User name logged with the remote socket address and authenticator classname for audit logging
            if (settings.enableAuditLog) {
                String address = ctx.channel().remoteAddress().toString();
                if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
                final String[] authClassParts = authenticator.getClass().toString().split("[.]");
                auditLogger.info("User {} with address {} authenticated by {}",
                        user.getName(), address, authClassParts[authClassParts.length - 1]);
            }
            // If we have got here we are authenticated so remove the handler and pass
            // the original message down the pipeline for processing
            ctx.pipeline().remove(this);
            final RequestMessage original = request.get();
            ctx.fireChannelRead(original);

            // Also send deferred requests if there are any down the pipeline for processing
            if (deferredRequests.get() != null) {
                deferredRequests.getAndSet(null).getValue().forEach(ctx::fireChannelRead);
            }
        } catch (AuthenticationException ae) {
            respondWithError(
                    requestMessage,
                    builder -> builder.statusMessage(ae.getMessage()).code(ResponseStatusCode.UNAUTHORIZED),
                    ctx);
        }
    }

    private void respondWithError(final RequestMessage requestMessage, final Function<ResponseMessage.Builder, ResponseMessage.Builder> buildResponse, final ChannelHandlerContext ctx) {
        final Attribute<RequestMessage> originalRequest = ctx.channel().attr(StateKey.REQUEST_MESSAGE);
        final Attribute<Pair<LocalDateTime, List<RequestMessage>>> deferredRequests = ctx.channel().attr(StateKey.DEFERRED_REQUEST_MESSAGES);

        if (!requestMessage.getOp().equals(Tokens.OPS_AUTHENTICATION)) {
            ctx.write(buildResponse.apply(ResponseMessage.build(requestMessage)).create());
        }

        if (originalRequest.get() != null) {
            ctx.write(buildResponse.apply(ResponseMessage.build(originalRequest.get())).create());
        }

        if (deferredRequests.get() != null) {
            deferredRequests
                    .getAndSet(null).getValue().stream()
                    .map(ResponseMessage::build)
                    .map(buildResponse)
                    .map(ResponseMessage.Builder::create)
                    .forEach(ctx::write);
        }

        ctx.flush();
    }

    private InetAddress getRemoteInetAddress(final ChannelHandlerContext ctx) {
        final Channel channel = ctx.channel();

        if (null == channel)
            return null;

        final SocketAddress genericSocketAddr = channel.remoteAddress();

        if (!(genericSocketAddr instanceof InetSocketAddress))
            return null;

        return ((InetSocketAddress) genericSocketAddr).getAddress();
    }
}
