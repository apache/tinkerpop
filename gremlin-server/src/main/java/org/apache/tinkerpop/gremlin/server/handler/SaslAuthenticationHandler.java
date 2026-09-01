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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.concurrent.ScheduledFuture;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
    /**
     * Default for {@code settings.authentication.preAuthTimeout}, how long a channel may stay unauthenticated,
     * covering the whole handshake, not just the deferral window.
     */
    public static final Duration MAX_REQUEST_DEFERRABLE_DURATION = Duration.ofSeconds(30);
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

        // rejected here so that a bad configuration fails startup rather than a request
        if (settings.authentication.maxDeferredRequests < 1)
            throw new IllegalStateException(String.format(
                    "authentication.maxDeferredRequests must be greater than zero but was %s",
                    settings.authentication.maxDeferredRequests));

        if (settings.authentication.maxPreAuthRetainedBytes < 1)
            throw new IllegalStateException(String.format(
                    "authentication.maxPreAuthRetainedBytes must be greater than zero but was %s",
                    settings.authentication.maxPreAuthRetainedBytes));

        if (settings.authentication.preAuthTimeout < 1)
            throw new IllegalStateException(String.format(
                    "authentication.preAuthTimeout must be greater than zero but was %s",
                    settings.authentication.preAuthTimeout));

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
        final Attribute<Long> retainedBytes = ctx.channel().attr(StateKey.DEFERRED_REQUEST_BYTES);

        final long maxRetainedBytes = settings.authentication.maxPreAuthRetainedBytes;

        if (negotiator.get() == null) {
            final long incomingRequestSize = incomingRequestSize(ctx.channel());

            // the request is held until authentication completes, so on its own it has to fit what may be retained
            final boolean tooLargeToRetain = incomingRequestSize > maxRetainedBytes;

            if (tooLargeToRetain) {
                logger.debug("Not retaining the {} byte request from {} pending authentication - {} bytes maximum",
                        incomingRequestSize, ctx.channel().remoteAddress(), maxRetainedBytes);
            }

            try {
                // First time through so save the request and send an AUTHENTICATE challenge with no data
                negotiator.set(authenticator.newSaslNegotiator(getRemoteInetAddress(ctx)));

                // retention starts here, so the deadline on it does too
                armDeadline(ctx);

                if (!tooLargeToRetain) {
                    request.set(requestMessage);
                    retainedBytes.set(incomingRequestSize);
                }

                final ResponseMessage authenticate = ResponseMessage.build(requestMessage)
                        .code(ResponseStatusCode.AUTHENTICATE).create();
                ctx.writeAndFlush(authenticate);

                // answered after the challenge, so authentication can still complete and the request be resent
                if (tooLargeToRetain) {
                    ctx.writeAndFlush(ResponseMessage.build(requestMessage)
                            .statusMessage("Request is too large to hold pending authentication (" + maxRetainedBytes + " bytes maximum).")
                            .code(ResponseStatusCode.UNAUTHORIZED).create());
                }
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
            final Pair<LocalDateTime, List<RequestMessage>> deferred = deferredRequests.get();

            // bounds what an unauthenticated channel can make the server retain, by count and by size
            final long alreadyRetainedBytes = retainedBytes.get() == null ? 0L : retainedBytes.get();
            final long incomingRequestSize = incomingRequestSize(ctx.channel());

            final int maxDeferredRequests = settings.authentication.maxDeferredRequests;

            final String breachMessage;
            if (deferred != null && deferred.getValue().size() >= maxDeferredRequests) {
                breachMessage = "Too many requests were deferred pending authentication (" + maxDeferredRequests + " maximum).";
            } else if (alreadyRetainedBytes + incomingRequestSize > maxRetainedBytes) {
                breachMessage = "Too many bytes were retained pending authentication (" + maxRetainedBytes + " bytes maximum).";
            } else {
                breachMessage = null;
            }

            if (breachMessage != null) {
                logger.debug("Rejecting the request from {} - {}", ctx.channel().remoteAddress(), breachMessage);

                // dropped rather than buffered for a peer that has stopped reading
                if (ctx.channel().isWritable()) {
                    ctx.writeAndFlush(ResponseMessage.build(requestMessage)
                            .statusMessage(breachMessage).code(ResponseStatusCode.UNAUTHORIZED).create());
                }
                return;
            }

            if (deferred == null) {
                deferredRequests.set(new ImmutablePair<>(LocalDateTime.now(), new ArrayList<>()));
            } else if (Duration.between(deferred.getKey(), LocalDateTime.now()).toMillis() > preAuthTimeout()) {
                // answered here rather than deferred, so that one request id cannot be answered twice
                respondWithError(requestMessage, this::didNotFinishInTime, ctx);
                return;
            }

            deferredRequests.get().getValue().add(requestMessage);
            retainedBytes.set(alreadyRetainedBytes + incomingRequestSize);

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
            cancelDeadline(ctx.channel());
            // User name logged with the remote socket address and authenticator classname for audit logging
            if (settings.enableAuditLog) {
                String address = ctx.channel().remoteAddress().toString();
                if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
                final String[] authClassParts = authenticator.getClass().toString().split("[.]");
                auditLogger.info("User {} with address {} authenticated by {}",
                        user.getName(), address, authClassParts[authClassParts.length - 1]);
            }
            final List<RequestMessage> deferred = releaseDeferredRequests(ctx.channel());

            // If we have got here we are authenticated so remove the handler and pass
            // the original message down the pipeline for processing
            ctx.pipeline().remove(this);
            final RequestMessage original = request.get();

            // null when an earlier failed attempt already answered it
            if (original != null) {
                ctx.fireChannelRead(original);
            }

            // Also send deferred requests if there are any down the pipeline for processing
            deferred.forEach(ctx::fireChannelRead);
        } catch (AuthenticationException ae) {
            respondWithError(
                    requestMessage,
                    builder -> builder.statusMessage(ae.getMessage()).code(ResponseStatusCode.UNAUTHORIZED),
                    ctx);
        }
    }

    /**
     * Milliseconds a channel may stay unauthenticated.
     */
    private long preAuthTimeout() {
        return settings.authentication.preAuthTimeout;
    }

    /**
     * Schedules the task that ends an authentication that does not finish in {@link #preAuthTimeout()}.
     */
    private void armDeadline(final ChannelHandlerContext ctx) {
        final Channel channel = ctx.channel();
        final Attribute<ScheduledFuture<?>> deadline = channel.attr(StateKey.PREAUTH_DEADLINE);

        // one task and one close listener per channel, however often a null negotiator sends us back here
        if (deadline.get() != null) return;

        // so that the set below can never orphan a live task
        cancelDeadline(channel);

        deadline.set(channel.eventLoop().schedule(() -> expireDeadline(ctx),
                preAuthTimeout(), TimeUnit.MILLISECONDS));
        channel.closeFuture().addListener(future -> cancelDeadline(channel));
    }

    /**
     * Cancels the deadline, if one is armed. Only authentication succeeding and the channel closing get here.
     */
    private static void cancelDeadline(final Channel channel) {
        final ScheduledFuture<?> deadline = channel.attr(StateKey.PREAUTH_DEADLINE).getAndSet(null);

        if (deadline != null) deadline.cancel(false);
    }

    /**
     * Answers whatever the channel still has pending and closes it.
     */
    private void expireDeadline(final ChannelHandlerContext ctx) {
        final Channel channel = ctx.channel();

        if (channel.attr(StateKey.AUTHENTICATED_USER).get() != null || !channel.isActive()) return;

        logger.debug("Closing the channel to {} - authentication did not finish in {} ms",
                channel.remoteAddress(), preAuthTimeout());

        answerPendingRequests(this::didNotFinishInTime, ctx);

        // unconditional, as a peer that never accepts the responses must not be able to hold the channel open
        ctx.close();
    }

    private ResponseMessage.Builder didNotFinishInTime(final ResponseMessage.Builder builder) {
        return builder.statusMessage("Authentication did not finish in the allowed duration (" + preAuthTimeout() + " ms).")
                .code(ResponseStatusCode.UNAUTHORIZED);
    }

    /**
     * Size of the frame the request being processed was decoded from, or zero when the transport recorded none.
     */
    private static long incomingRequestSize(final Channel channel) {
        final Integer requestSize = channel.attr(StateKey.REQUEST_SIZE).get();

        return requestSize == null ? 0L : requestSize;
    }

    /**
     * Drops the running byte total, returning the requests the channel had deferred.
     */
    private static List<RequestMessage> releaseDeferredRequests(final Channel channel) {
        final Pair<LocalDateTime, List<RequestMessage>> deferred =
                channel.attr(StateKey.DEFERRED_REQUEST_MESSAGES).getAndSet(null);
        channel.attr(StateKey.DEFERRED_REQUEST_BYTES).set(0L);

        return deferred == null ? Collections.emptyList() : deferred.getValue();
    }

    /**
     * Answers the stashed and deferred requests, releasing both, plus {@code requestMessage} unless it is an
     * authentication request.
     */
    private void respondWithError(final RequestMessage requestMessage, final Function<ResponseMessage.Builder, ResponseMessage.Builder> buildResponse, final ChannelHandlerContext ctx) {
        if (!requestMessage.getOp().equals(Tokens.OPS_AUTHENTICATION)) {
            ctx.write(buildResponse.apply(ResponseMessage.build(requestMessage)).create());
        }

        answerPendingRequests(buildResponse, ctx);
    }

    /**
     * Answers the stashed and deferred requests, releasing both.
     */
    private static void answerPendingRequests(final Function<ResponseMessage.Builder, ResponseMessage.Builder> buildResponse, final ChannelHandlerContext ctx) {
        final Attribute<RequestMessage> originalRequest = ctx.channel().attr(StateKey.REQUEST_MESSAGE);

        // cleared as it is answered so that one request id cannot be answered twice
        final RequestMessage stashedRequest = originalRequest.getAndSet(null);

        if (stashedRequest != null) {
            ctx.write(buildResponse.apply(ResponseMessage.build(stashedRequest)).create());
        }

        // this also drops the stashed request's share of the retained byte total
        releaseDeferredRequests(ctx.channel()).stream()
                .map(ResponseMessage::build)
                .map(buildResponse)
                .map(ResponseMessage.Builder::create)
                .forEach(ctx::write);

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
