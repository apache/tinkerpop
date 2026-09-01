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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.util.DefaultAttributeMap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SaslAuthenticationHandlerTest {

    // the size a request is given when the test does not care about the byte budget
    private static final int SMALL_REQUEST_SIZE = 200;

    // a real AttributeMap backs the channel attributes the handler reads and writes
    private final DefaultAttributeMap attributes = new DefaultAttributeMap();
    private final Channel channel = Mockito.mock(Channel.class);
    private final ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    private final Authenticator authenticator = Mockito.mock(Authenticator.class);
    private final Authenticator.SaslNegotiator negotiator = Mockito.mock(Authenticator.SaslNegotiator.class);
    private final Settings settings = new Settings();
    private final SaslAuthenticationHandler handler =
            new SaslAuthenticationHandler(authenticator, null, settings);

    // the limits the handler reads for this channel, left at their defaults unless a test sets one
    private final long budget = settings.authentication.maxPreAuthRetainedBytes;
    private final int maxDeferred = settings.authentication.maxDeferredRequests;
    private final long preAuthTimeout = settings.authentication.preAuthTimeout;

    private final EventLoop eventLoop = Mockito.mock(EventLoop.class);
    private final ChannelFuture closeFuture = Mockito.mock(ChannelFuture.class);
    private final ChannelFuture writeFuture = Mockito.mock(ChannelFuture.class);
    private final ScheduledFuture<?> deadlineFuture = Mockito.mock(ScheduledFuture.class);

    // what the handler handed to the event loop and to the close future, so that the test can run them
    private final List<Runnable> scheduledTasks = new ArrayList<>();
    private final List<GenericFutureListener<Future<? super Void>>> closeListeners = new ArrayList<>();

    @Before
    public void setupForEachTest() {
        Mockito.when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 45678));
        Mockito.doAnswer(invocation -> attributes.attr(invocation.getArgument(0))).when(channel).attr(Mockito.any());
        Mockito.when(channel.isActive()).thenReturn(true);
        Mockito.when(channel.isWritable()).thenReturn(true);
        Mockito.when(channel.eventLoop()).thenReturn(eventLoop);
        Mockito.when(channel.closeFuture()).thenReturn(closeFuture);
        Mockito.doAnswer(invocation -> {
            scheduledTasks.add(invocation.getArgument(0));
            return deadlineFuture;
        }).when(eventLoop).schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
        Mockito.doAnswer(invocation -> {
            closeListeners.add(invocation.getArgument(0));
            return closeFuture;
        }).when(closeFuture).addListener(Mockito.any());
        Mockito.when(ctx.channel()).thenReturn(channel);
        Mockito.when(ctx.write(Mockito.any())).thenReturn(writeFuture);
        Mockito.when(ctx.writeAndFlush(Mockito.any())).thenReturn(writeFuture);
        Mockito.when(authenticator.newSaslNegotiator(Mockito.any(InetAddress.class))).thenReturn(negotiator);
    }

    @Test
    public void shouldErrorOnlyTheRequestThatBreachedTheCountCap() throws Exception {
        final RequestMessage stashed = startAuthentication(SMALL_REQUEST_SIZE);

        RequestMessage breaching = null;
        for (int ix = 0; ix < maxDeferred + 1; ix++) {
            breaching = deferRequest(SMALL_REQUEST_SIZE);
        }

        // the AUTHENTICATE challenge is followed by one error, for the breaching request
        final List<ResponseMessage> flushed = flushedResponses();
        assertEquals(2, flushed.size());
        assertEquals(ResponseStatusCode.AUTHENTICATE, flushed.get(0).getStatus().getCode());

        final ResponseMessage error = flushed.get(1);
        assertEquals(breaching.getRequestId(), error.getRequestId());
        assertEquals(ResponseStatusCode.UNAUTHORIZED, error.getStatus().getCode());
        assertTrue(error.getStatus().getMessage().contains(maxDeferred + " maximum"));
        Mockito.verify(ctx, Mockito.never()).write(Mockito.any());

        // the list stays at the cap and the breaching request was never appended
        assertEquals(maxDeferred, deferredRequests().getValue().size());
        assertFalse(deferredRequests().getValue().contains(breaching));
        assertEquals((maxDeferred + 1) * (long) SMALL_REQUEST_SIZE, retainedBytes());

        // the request that started authentication is still stashed for release once authentication completes
        assertSame(stashed, attributes.attr(StateKey.REQUEST_MESSAGE).get());

        // the channel is left open so authentication can still complete, leaving the deadline as all that ends it
        Mockito.verify(ctx, Mockito.never()).close();
        assertDeadlineStillArmed();
    }

    @Test
    public void shouldErrorOnlyTheRequestThatBreachedTheByteBudget() throws Exception {
        startAuthentication(SMALL_REQUEST_SIZE);

        // two requests fill what is left of the budget and are far short of the cap on the number of requests
        final int half = (int) (budget - SMALL_REQUEST_SIZE) / 2;
        deferRequest(half);
        deferRequest(half);
        assertEquals(budget, retainedBytes());

        // the check includes the incoming request, otherwise one request could pass the budget
        final RequestMessage breaching = deferRequest(1);

        final List<ResponseMessage> flushed = flushedResponses();
        assertEquals(2, flushed.size());
        final ResponseMessage error = flushed.get(1);
        assertEquals(breaching.getRequestId(), error.getRequestId());
        assertEquals(ResponseStatusCode.UNAUTHORIZED, error.getStatus().getCode());
        assertTrue(error.getStatus().getMessage().contains(budget + " bytes maximum"));

        // only the breaching request was rejected and the channel is left open
        assertEquals(2, deferredRequests().getValue().size());
        assertEquals(budget, retainedBytes());
        Mockito.verify(ctx, Mockito.never()).write(Mockito.any());
        Mockito.verify(ctx, Mockito.never()).close();

        // refusing a request must not disarm the deadline, or breaching the budget would hold the channel open
        assertDeadlineStillArmed();
    }

    @Test
    public void shouldChargeTheStashedRequestToTheByteBudget() throws Exception {
        final int stashSize = (int) budget - 100;
        startAuthentication(stashSize);

        // the request held in StateKey.REQUEST_MESSAGE counts from the moment it is stashed
        assertEquals(stashSize, retainedBytes());

        // so only the remainder of the budget is left to defer into
        final RequestMessage tooLarge = deferRequest(101);
        assertNull(deferredRequests());

        final List<ResponseMessage> flushed = flushedResponses();
        assertEquals(2, flushed.size());
        assertEquals(tooLarge.getRequestId(), flushed.get(1).getRequestId());
        assertEquals(ResponseStatusCode.UNAUTHORIZED, flushed.get(1).getStatus().getCode());

        final RequestMessage fits = deferRequest(100);
        assertEquals(1, deferredRequests().getValue().size());
        assertSame(fits, deferredRequests().getValue().get(0));
        assertEquals(budget, retainedBytes());
        Mockito.verify(ctx, Mockito.never()).close();
    }

    @Test
    public void shouldChallengeButRetainNothingForAFirstRequestTooLargeToStash() throws Exception {
        final RequestMessage tooLarge = startAuthenticationWithOversizedRequest();

        // authentication still starts, so the challenge goes out first and the request is answered after it
        final List<ResponseMessage> flushed = flushedResponses();
        assertEquals(2, flushed.size());
        assertEquals(tooLarge.getRequestId(), flushed.get(0).getRequestId());
        assertEquals(ResponseStatusCode.AUTHENTICATE, flushed.get(0).getStatus().getCode());
        assertEquals(tooLarge.getRequestId(), flushed.get(1).getRequestId());
        assertEquals(ResponseStatusCode.UNAUTHORIZED, flushed.get(1).getStatus().getCode());
        assertTrue(flushed.get(1).getStatus().getMessage().contains("too large"));

        // it was answered rather than stashed, and nothing was retained for the channel either
        assertNull(attributes.attr(StateKey.REQUEST_MESSAGE).get());
        assertNull(deferredRequests());
        assertEquals(0L, retainedBytes());
        Mockito.verify(ctx, Mockito.never()).write(Mockito.any());
        Mockito.verify(ctx, Mockito.never()).close();
    }

    @Test
    public void shouldDeferANormalSizedRequestAfterAFirstRequestTooLargeToStashWasRejected() throws Exception {
        startAuthenticationWithOversizedRequest();

        final RequestMessage deferred = deferRequest(SMALL_REQUEST_SIZE);

        // only the deferred request is charged, as the rejected one was never counted
        assertEquals(1, deferredRequests().getValue().size());
        assertSame(deferred, deferredRequests().getValue().get(0));
        assertEquals(SMALL_REQUEST_SIZE, retainedBytes());

        completeAuthentication();

        final List<Object> fired = firedReads();
        assertEquals(1, fired.size());
        assertSame(deferred, fired.get(0));
        Mockito.verify(ctx, Mockito.never()).write(Mockito.any());
        assertNothingLeftToAnswer();
    }

    @Test
    public void shouldAnswerTheStashedRequestExactlyOnceAcrossRepeatedErrors() throws Exception {
        final RequestMessage stashed = startAuthentication(SMALL_REQUEST_SIZE);
        final RequestMessage deferred = deferRequest(SMALL_REQUEST_SIZE);

        failAuthentication();

        final List<ResponseMessage> errors = writtenResponses();
        assertEquals(2, errors.size());
        assertEquals(stashed.getRequestId(), errors.get(0).getRequestId());
        assertEquals(deferred.getRequestId(), errors.get(1).getRequestId());
        errors.forEach(error -> assertEquals(ResponseStatusCode.UNAUTHORIZED, error.getStatus().getCode()));
        assertNull("the stashed request must be cleared as it is answered",
                attributes.attr(StateKey.REQUEST_MESSAGE).get());
        assertNothingLeftToAnswer();

        // an authentication request with no sasl argument takes respondWithError again, with nothing left to answer
        handler.channelRead(ctx, RequestMessage.build(Tokens.OPS_AUTHENTICATION).create());

        assertEquals(2, writtenResponses().size());
        assertEquals(1L, answersFor(stashed));
        Mockito.verify(ctx, Mockito.never()).close();
    }

    @Test
    public void shouldNotAnswerTheStashedRequestAgainWhenAuthenticationLaterSucceeds() throws Exception {
        final RequestMessage stashed = startAuthentication(SMALL_REQUEST_SIZE);
        deferRequest(SMALL_REQUEST_SIZE);

        failAuthentication();
        assertNull(attributes.attr(StateKey.REQUEST_MESSAGE).get());

        completeAuthentication();

        // the failure already answered it, so nothing more goes out for it and nothing null goes down the pipeline
        assertEquals(1L, answersFor(stashed));
        assertTrue(firedReads().isEmpty());
        Mockito.verify(ctx, Mockito.never()).fireChannelRead(null);
        assertNothingLeftToAnswer();
    }

    @Test
    public void shouldRetainNothingOnceAuthenticationSucceeds() throws Exception {
        final RequestMessage stashed = startAuthentication(SMALL_REQUEST_SIZE);
        final RequestMessage deferred = deferRequest(SMALL_REQUEST_SIZE);

        completeAuthentication();

        final List<Object> fired = firedReads();
        assertEquals(2, fired.size());
        assertSame(stashed, fired.get(0));
        assertSame(deferred, fired.get(1));
        Mockito.verify(ctx, Mockito.never()).write(Mockito.any());
        assertNull(deferredRequests());
        assertEquals(0L, retainedBytes());
    }

    @Test
    public void shouldRetainNothingWhenTheSaslArgumentIsMissing() throws Exception {
        final RequestMessage stashed = startAuthentication(SMALL_REQUEST_SIZE);
        final RequestMessage deferred = deferRequest(SMALL_REQUEST_SIZE);

        handler.channelRead(ctx, RequestMessage.build(Tokens.OPS_AUTHENTICATION).create());

        // the authentication request itself is not answered, the stashed and deferred ones are
        final List<ResponseMessage> errors = writtenResponses();
        assertEquals(2, errors.size());
        assertEquals(stashed.getRequestId(), errors.get(0).getRequestId());
        assertEquals(deferred.getRequestId(), errors.get(1).getRequestId());
        errors.forEach(error -> assertEquals(ResponseStatusCode.UNAUTHORIZED, error.getStatus().getCode()));
        assertNothingLeftToAnswer();
    }

    @Test
    public void shouldRetainNothingWhenTheSaslArgumentIsNotAString() throws Exception {
        final RequestMessage stashed = startAuthentication(SMALL_REQUEST_SIZE);
        final RequestMessage deferred = deferRequest(SMALL_REQUEST_SIZE);

        handler.channelRead(ctx, RequestMessage.build(Tokens.OPS_AUTHENTICATION)
                .addArg(Tokens.ARGS_SASL, 1234).create());

        final List<ResponseMessage> errors = writtenResponses();
        assertEquals(2, errors.size());
        assertEquals(stashed.getRequestId(), errors.get(0).getRequestId());
        assertEquals(deferred.getRequestId(), errors.get(1).getRequestId());
        errors.forEach(error ->
                assertEquals(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST, error.getStatus().getCode()));
        assertNothingLeftToAnswer();
    }

    @Test
    public void shouldAnswerRatherThanDeferARequestOnceTheDeferrableDurationHasPassed() throws Exception {
        final RequestMessage stashed = startAuthentication(SMALL_REQUEST_SIZE);
        final RequestMessage deferred = deferRequest(SMALL_REQUEST_SIZE);

        // the deferral window opened longer ago than the allowed duration, which the next arrival notices
        attributes.attr(StateKey.DEFERRED_REQUEST_MESSAGES).set(new ImmutablePair<>(
                LocalDateTime.now().minus(Duration.ofMillis(preAuthTimeout)).minusSeconds(1),
                deferredRequests().getValue()));

        final RequestMessage late = deferRequest(SMALL_REQUEST_SIZE);

        // the late request is answered as an arrival, never appended, so no request id is answered twice
        final List<ResponseMessage> errors = writtenResponses();
        assertEquals(3, errors.size());
        assertEquals(late.getRequestId(), errors.get(0).getRequestId());
        assertEquals(stashed.getRequestId(), errors.get(1).getRequestId());
        assertEquals(deferred.getRequestId(), errors.get(2).getRequestId());
        errors.forEach(error -> assertEquals(ResponseStatusCode.UNAUTHORIZED, error.getStatus().getCode()));
        assertEquals(1L, answersFor(late));
        assertEquals(1L, answersFor(stashed));
        assertEquals(1L, answersFor(deferred));
        assertNothingLeftToAnswer();

        // the stale window answers rather than closes, so the deadline is still what ends the channel
        Mockito.verify(ctx, Mockito.never()).close();
        assertDeadlineStillArmed();
    }

    @Test
    public void shouldArmNothingWhenTheAuthenticatorIsNotReady() throws Exception {
        Mockito.when(authenticator.newSaslNegotiator(Mockito.any(InetAddress.class)))
                .thenThrow(new IllegalStateException("not ready"));

        final RequestMessage first = RequestMessage.build(Tokens.OPS_EVAL).create();
        attributes.attr(StateKey.REQUEST_SIZE).set(SMALL_REQUEST_SIZE);
        handler.channelRead(ctx, first);

        final List<ResponseMessage> errors = writtenResponses();
        assertEquals(1, errors.size());
        assertEquals(first.getRequestId(), errors.get(0).getRequestId());
        assertEquals(ResponseStatusCode.SERVER_ERROR, errors.get(0).getStatus().getCode());

        // no negotiator means nothing is retained, so no deadline is left running either
        assertNull(attributes.attr(StateKey.NEGOTIATOR).get());
        assertNull(deadline());
        assertTrue(scheduledTasks.isEmpty());
        assertNothingLeftToAnswer();
        Mockito.verify(ctx, Mockito.never()).close();
    }

    @Test
    public void shouldCountNothingForARequestTheTransportDidNotSize() throws Exception {
        // StateKey.REQUEST_SIZE is absent on transports that do not record it
        assertNull(attributes.attr(StateKey.REQUEST_SIZE).get());

        final RequestMessage first = RequestMessage.build(Tokens.OPS_AUTHENTICATION).create();
        handler.channelRead(ctx, first);

        assertSame(first, attributes.attr(StateKey.REQUEST_MESSAGE).get());
        assertEquals(0L, retainedBytes());

        handler.channelRead(ctx, RequestMessage.build(Tokens.OPS_EVAL).create());

        // the request is still deferred, and still bounded by the cap on the number of them
        assertEquals(1, deferredRequests().getValue().size());
        assertEquals(0L, retainedBytes());
        assertEquals(1, flushedResponses().size());
    }

    @Test
    public void shouldArmTheDeadlineWhenTheNegotiatorIsCreated() throws Exception {
        startAuthentication(SMALL_REQUEST_SIZE);

        // retention begins with the stashed request, before anything is deferred, so the deadline is already armed
        assertSame(deadlineFuture, deadline());
        assertNull(deferredRequests());
        assertEquals(1, scheduledTasks.size());
        Mockito.verify(eventLoop).schedule(Mockito.any(Runnable.class),
                Mockito.eq(preAuthTimeout), Mockito.eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void shouldNotArmTheDeadlineAgainWhenTheDeferredRequestsAreRefilled() throws Exception {
        startAuthentication(SMALL_REQUEST_SIZE);
        deferRequest(SMALL_REQUEST_SIZE);

        // releases the deferred requests, so the next arrival opens a fresh window on the same channel
        failAuthentication();
        assertNull(deferredRequests());

        deferRequest(SMALL_REQUEST_SIZE);

        assertEquals(1, deferredRequests().getValue().size());
        assertEquals(1, scheduledTasks.size());
        assertEquals(1, closeListeners.size());
        assertDeadlineStillArmed();
    }

    @Test
    public void shouldArmTheDeadlineOnceWhenTheNegotiatorKeepsComingBackNull() throws Exception {
        Mockito.when(authenticator.newSaslNegotiator(Mockito.any(InetAddress.class))).thenReturn(null);

        for (int ix = 0; ix < 3; ix++) {
            attributes.attr(StateKey.REQUEST_SIZE).set(SMALL_REQUEST_SIZE);
            handler.channelRead(ctx, RequestMessage.build(Tokens.OPS_AUTHENTICATION).create());
        }

        // the branch that creates the negotiator runs again for each request, and must arm nothing more
        assertNull(attributes.attr(StateKey.NEGOTIATOR).get());
        assertEquals(3, flushedResponses().size());
        assertEquals(1, scheduledTasks.size());
        assertEquals(1, closeListeners.size());
        assertDeadlineStillArmed();
    }

    /**
     * The paths that refuse a request rather than fail an authentication one keep the deadline armed in
     * {@link #shouldErrorOnlyTheRequestThatBreachedTheCountCap},
     * {@link #shouldErrorOnlyTheRequestThatBreachedTheByteBudget} and
     * {@link #shouldAnswerRatherThanDeferARequestOnceTheDeferrableDurationHasPassed}.
     */
    @Test
    public void shouldKeepTheDeadlineArmedThroughEveryFailedAuthenticationRequest() throws Exception {
        startAuthentication(SMALL_REQUEST_SIZE);
        deferRequest(SMALL_REQUEST_SIZE);

        // an authentication request with no sasl argument
        handler.channelRead(ctx, RequestMessage.build(Tokens.OPS_AUTHENTICATION).create());
        assertEquals(2, writtenResponses().size());
        assertDeadlineStillArmed();

        // an authentication request whose sasl argument is not a String
        deferRequest(SMALL_REQUEST_SIZE);
        handler.channelRead(ctx, RequestMessage.build(Tokens.OPS_AUTHENTICATION)
                .addArg(Tokens.ARGS_SASL, 1234).create());
        assertEquals(3, writtenResponses().size());
        assertDeadlineStillArmed();

        // an authentication request the negotiator rejects
        deferRequest(SMALL_REQUEST_SIZE);
        failAuthentication();
        assertEquals(4, writtenResponses().size());
        assertDeadlineStillArmed();

        // none of them closed the channel either, so the deadline is all that ends it
        Mockito.verify(ctx, Mockito.never()).close();
    }

    @Test
    public void shouldCancelTheDeadlineWhenAuthenticationSucceeds() throws Exception {
        startAuthentication(SMALL_REQUEST_SIZE);

        completeAuthentication();

        assertNull(deadline());
        Mockito.verify(deadlineFuture).cancel(false);
    }

    @Test
    public void shouldCancelTheDeadlineWhenTheChannelCloses() throws Exception {
        startAuthentication(SMALL_REQUEST_SIZE);
        deferRequest(SMALL_REQUEST_SIZE);

        closeChannel();

        assertNull(deadline());
        Mockito.verify(deadlineFuture).cancel(false);
    }

    @Test
    public void shouldAnswerEachPendingRequestExactlyOnceAndCloseWhenTheDeadlineExpires() throws Exception {
        final RequestMessage stashed = startAuthentication(SMALL_REQUEST_SIZE);
        final RequestMessage first = deferRequest(SMALL_REQUEST_SIZE);
        final RequestMessage second = deferRequest(SMALL_REQUEST_SIZE);

        runDeadline();

        // closed as soon as the responses are flushed, never gated on a write completing
        Mockito.verify(ctx).flush();
        Mockito.verify(ctx).close();
        Mockito.verify(writeFuture, Mockito.never()).addListener(Mockito.any());

        final List<ResponseMessage> errors = writtenResponses();
        assertEquals(3, errors.size());
        assertEquals(stashed.getRequestId(), errors.get(0).getRequestId());
        assertEquals(first.getRequestId(), errors.get(1).getRequestId());
        assertEquals(second.getRequestId(), errors.get(2).getRequestId());
        errors.forEach(error -> assertEquals(ResponseStatusCode.UNAUTHORIZED, error.getStatus().getCode()));
        assertEquals(1L, answersFor(stashed));
        assertEquals(1L, answersFor(first));
        assertEquals(1L, answersFor(second));
        assertNothingLeftToAnswer();
        assertTrue(firedReads().isEmpty());
    }

    @Test
    public void shouldNotAnswerARequestAnErrorAlreadyAnsweredWhenTheDeadlineExpires() throws Exception {
        final RequestMessage stashed = startAuthentication(SMALL_REQUEST_SIZE);
        final RequestMessage deferred = deferRequest(SMALL_REQUEST_SIZE);

        failAuthentication();
        assertEquals(2, writtenResponses().size());

        runDeadline();

        // both were answered and cleared by the failure, so the deadline has nothing left to answer
        assertEquals(2, writtenResponses().size());
        assertEquals(1L, answersFor(stashed));
        assertEquals(1L, answersFor(deferred));

        // the channel is still unauthenticated, so it is still closed
        Mockito.verify(ctx).close();
    }

    @Test
    public void shouldNotCloseAnAuthenticatedChannelWhenTheDeadlineExpires() throws Exception {
        startAuthentication(SMALL_REQUEST_SIZE);
        completeAuthentication();

        // a task already running when the cancel came in still has to find the channel authenticated and stop
        runDeadline();

        Mockito.verify(ctx, Mockito.never()).close();
        Mockito.verify(ctx, Mockito.never()).write(Mockito.any());
    }

    @Test
    public void shouldNotCloseAnInactiveChannelWhenTheDeadlineExpires() throws Exception {
        startAuthentication(SMALL_REQUEST_SIZE);
        deferRequest(SMALL_REQUEST_SIZE);
        Mockito.when(channel.isActive()).thenReturn(false);

        runDeadline();

        Mockito.verify(ctx, Mockito.never()).close();
        Mockito.verify(ctx, Mockito.never()).write(Mockito.any());
    }

    @Test
    public void shouldSkipTheRejectionWriteWhenTheChannelIsNotWritable() throws Exception {
        startAuthentication(SMALL_REQUEST_SIZE);

        for (int ix = 0; ix < maxDeferred; ix++) {
            deferRequest(SMALL_REQUEST_SIZE);
        }

        Mockito.when(channel.isWritable()).thenReturn(false);
        final RequestMessage breaching = deferRequest(SMALL_REQUEST_SIZE);

        // the rejection is dropped rather than buffered, so the AUTHENTICATE challenge is all that went out
        assertEquals(1, flushedResponses().size());
        assertEquals(ResponseStatusCode.AUTHENTICATE, flushedResponses().get(0).getStatus().getCode());
        Mockito.verify(ctx, Mockito.never()).write(Mockito.any());

        // the request is still refused rather than deferred
        assertEquals(maxDeferred, deferredRequests().getValue().size());
        assertFalse(deferredRequests().getValue().contains(breaching));
        assertEquals((maxDeferred + 1) * (long) SMALL_REQUEST_SIZE, retainedBytes());
        Mockito.verify(ctx, Mockito.never()).close();
    }

    @Test
    public void shouldCapTheDeferredRequestsAtTheConfiguredCount() throws Exception {
        settings.authentication.maxDeferredRequests = 2;

        startAuthentication(SMALL_REQUEST_SIZE);
        deferRequest(SMALL_REQUEST_SIZE);
        deferRequest(SMALL_REQUEST_SIZE);

        // the default of 64 would have deferred this one
        final RequestMessage breaching = deferRequest(SMALL_REQUEST_SIZE);

        final List<ResponseMessage> flushed = flushedResponses();
        assertEquals(2, flushed.size());
        assertEquals(breaching.getRequestId(), flushed.get(1).getRequestId());
        assertEquals(ResponseStatusCode.UNAUTHORIZED, flushed.get(1).getStatus().getCode());
        assertTrue(flushed.get(1).getStatus().getMessage().contains("2 maximum"));
        assertEquals(2, deferredRequests().getValue().size());
        assertFalse(deferredRequests().getValue().contains(breaching));
    }

    @Test
    public void shouldBudgetTheRetainedBytesAtTheConfiguredTotal() throws Exception {
        settings.authentication.maxPreAuthRetainedBytes = 500L;

        startAuthentication(400);

        // the default budget of 2 MiB would have deferred this one
        final RequestMessage breaching = deferRequest(101);

        final List<ResponseMessage> flushed = flushedResponses();
        assertEquals(2, flushed.size());
        assertEquals(breaching.getRequestId(), flushed.get(1).getRequestId());
        assertEquals(ResponseStatusCode.UNAUTHORIZED, flushed.get(1).getStatus().getCode());
        assertTrue(flushed.get(1).getStatus().getMessage().contains("500 bytes maximum"));
        assertNull(deferredRequests());

        // what is left of the smaller budget is still deferrable
        final RequestMessage fits = deferRequest(100);
        assertSame(fits, deferredRequests().getValue().get(0));
        assertEquals(500L, retainedBytes());
    }

    @Test
    public void shouldArmTheDeadlineWithTheConfiguredTimeout() throws Exception {
        settings.authentication.preAuthTimeout = 1234L;

        startAuthentication(SMALL_REQUEST_SIZE);

        Mockito.verify(eventLoop).schedule(Mockito.any(Runnable.class),
                Mockito.eq(1234L), Mockito.eq(TimeUnit.MILLISECONDS));

        runDeadline();

        assertEquals(1, writtenResponses().size());
        assertTrue(writtenResponses().get(0).getStatus().getMessage().contains("1234 ms"));
    }

    @Test
    public void shouldRefuseAMaxDeferredRequestsThatIsNotPositive() {
        assertRejectedAtConstruction(s -> s.authentication.maxDeferredRequests = 0,
                "authentication.maxDeferredRequests");
        assertRejectedAtConstruction(s -> s.authentication.maxDeferredRequests = -1,
                "authentication.maxDeferredRequests");
    }

    @Test
    public void shouldRefuseAMaxPreAuthRetainedBytesThatIsNotPositive() {
        assertRejectedAtConstruction(s -> s.authentication.maxPreAuthRetainedBytes = 0L,
                "authentication.maxPreAuthRetainedBytes");
        assertRejectedAtConstruction(s -> s.authentication.maxPreAuthRetainedBytes = -1L,
                "authentication.maxPreAuthRetainedBytes");
    }

    @Test
    public void shouldRefuseAPreAuthTimeoutThatIsNotPositive() {
        assertRejectedAtConstruction(s -> s.authentication.preAuthTimeout = 0L,
                "authentication.preAuthTimeout");
        assertRejectedAtConstruction(s -> s.authentication.preAuthTimeout = -1L,
                "authentication.preAuthTimeout");
    }

    /**
     * Asserts that the handler cannot be constructed once {@code invalidSetting} has been applied, so that the server
     * fails to start rather than a request.
     */
    private void assertRejectedAtConstruction(final Consumer<Settings> invalidSetting, final String settingName) {
        final Settings invalid = new Settings();
        invalidSetting.accept(invalid);

        try {
            new SaslAuthenticationHandler(authenticator, null, invalid);
            fail("a value that is not positive for " + settingName + " must not be accepted");
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains(settingName + " must be greater than zero"));
        }
    }

    /**
     * Sends a first request so that later non-authentication requests take the deferral path, returning the request
     * the handler stashes in {@link StateKey#REQUEST_MESSAGE}.
     */
    private RequestMessage startAuthentication(final int requestSize) throws Exception {
        final RequestMessage first = RequestMessage.build(Tokens.OPS_AUTHENTICATION).create();
        attributes.attr(StateKey.REQUEST_SIZE).set(requestSize);
        handler.channelRead(ctx, first);

        assertNotNull(attributes.attr(StateKey.NEGOTIATOR).get());

        return first;
    }

    /**
     * Sends a first request too large to be retained, which still starts authentication.
     */
    private RequestMessage startAuthenticationWithOversizedRequest() throws Exception {
        final RequestMessage first = RequestMessage.build(Tokens.OPS_EVAL).create();
        attributes.attr(StateKey.REQUEST_SIZE).set((int) budget + 1);
        handler.channelRead(ctx, first);

        assertNotNull(attributes.attr(StateKey.NEGOTIATOR).get());

        return first;
    }

    /**
     * Sends a request that takes the deferral path, sized as the decoders size it.
     */
    private RequestMessage deferRequest(final int requestSize) throws Exception {
        final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL).create();
        attributes.attr(StateKey.REQUEST_SIZE).set(requestSize);
        handler.channelRead(ctx, request);

        return request;
    }

    /**
     * Sends a SASL response that the negotiator accepts, releasing the deferred requests down the pipeline.
     */
    private void completeAuthentication() throws Exception {
        // doReturn() rather than when() so that it can follow failAuthentication() in the one test
        Mockito.when(ctx.pipeline()).thenReturn(Mockito.mock(ChannelPipeline.class));
        Mockito.doReturn(new byte[0]).when(negotiator).evaluateResponse(Mockito.any());
        Mockito.doReturn(true).when(negotiator).isComplete();
        Mockito.doReturn(new AuthenticatedUser("stephen")).when(negotiator).getAuthenticatedUser();

        sendSaslResponse();
    }

    /**
     * Sends a SASL response that the negotiator rejects.
     */
    private void failAuthentication() throws Exception {
        Mockito.doThrow(new AuthenticationException("Username and/or password are incorrect"))
                .when(negotiator).evaluateResponse(Mockito.any());

        sendSaslResponse();
    }

    private void sendSaslResponse() throws Exception {
        attributes.attr(StateKey.REQUEST_SIZE).set(SMALL_REQUEST_SIZE);
        handler.channelRead(ctx, RequestMessage.build(Tokens.OPS_AUTHENTICATION)
                .addArg(Tokens.ARGS_SASL, Base64.getEncoder().encodeToString(new byte[0])).create());
    }

    private void assertNothingLeftToAnswer() {
        assertNull(attributes.attr(StateKey.REQUEST_MESSAGE).get());
        assertNull(deferredRequests());
        assertEquals(0L, retainedBytes());
    }

    private void assertDeadlineStillArmed() {
        assertSame("the deadline must still be armed", deadlineFuture, deadline());
        Mockito.verify(deadlineFuture, Mockito.never()).cancel(Mockito.anyBoolean());
    }

    /**
     * Runs the task the handler scheduled, failing rather than doing nothing when there is not exactly one of them.
     */
    private void runDeadline() {
        assertEquals("exactly one deadline task must have been scheduled", 1, scheduledTasks.size());
        scheduledTasks.get(0).run();
    }

    /**
     * Fires the listener the handler registered on the close future, failing when there is not exactly one of them.
     */
    private void closeChannel() throws Exception {
        assertEquals("exactly one close listener must have been registered", 1, closeListeners.size());
        closeListeners.get(0).operationComplete(closeFuture);
    }

    private ScheduledFuture<?> deadline() {
        return attributes.attr(StateKey.PREAUTH_DEADLINE).get();
    }

    /**
     * Number of terminal responses written for {@code request}. The AUTHENTICATE challenge is not one of them and
     * leaves through {@code writeAndFlush()} rather than {@code write()}.
     */
    private long answersFor(final RequestMessage request) {
        return writtenResponses().stream()
                .filter(response -> request.getRequestId().equals(response.getRequestId()))
                .count();
    }

    private Pair<LocalDateTime, List<RequestMessage>> deferredRequests() {
        return attributes.attr(StateKey.DEFERRED_REQUEST_MESSAGES).get();
    }

    private long retainedBytes() {
        final Long bytes = attributes.attr(StateKey.DEFERRED_REQUEST_BYTES).get();
        return bytes == null ? 0L : bytes;
    }

    private List<ResponseMessage> flushedResponses() {
        final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(ctx, Mockito.atLeast(0)).writeAndFlush(captor.capture());
        return captor.getAllValues().stream().map(ResponseMessage.class::cast).collect(Collectors.toList());
    }

    private List<ResponseMessage> writtenResponses() {
        final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(ctx, Mockito.atLeast(0)).write(captor.capture());
        return captor.getAllValues().stream().map(ResponseMessage.class::cast).collect(Collectors.toList());
    }

    private List<Object> firedReads() {
        final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(ctx, Mockito.atLeast(0)).fireChannelRead(captor.capture());
        return captor.getAllValues();
    }
}
