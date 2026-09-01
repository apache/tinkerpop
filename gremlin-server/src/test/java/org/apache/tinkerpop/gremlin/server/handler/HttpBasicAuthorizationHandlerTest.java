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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HttpBasicAuthorizationHandlerTest {

    private final Authorizer authorizer = new Authorizer() {
        @Override
        public void setup(final Map<String, Object> config) throws AuthorizationException {
        }

        @Override
        public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode,
                                  final Map<String, String> aliases) throws AuthorizationException {
            return bytecode;
        }

        @Override
        public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
        }
    };

    @Test
    public void shouldHandleRejectedRequestWithTheUserFromItsChannel() throws Exception {
        final CountDownLatch firstRequestInAuthorizer = new CountDownLatch(1);
        final CountDownLatch continueFirstRequest = new CountDownLatch(1);
        final BlockingAuthorizer authorizer =
                new BlockingAuthorizer(firstRequestInAuthorizer, continueFirstRequest);
        final HttpBasicAuthorizationHandler handler = new HttpBasicAuthorizationHandler(authorizer);
        final EmbeddedChannel firstChannel = new EmbeddedChannel(handler);
        final EmbeddedChannel secondChannel = new EmbeddedChannel(handler);
        final RecordingUser firstUser = new RecordingUser("first");
        final RecordingUser secondUser = new RecordingUser("second");
        firstChannel.attr(StateKey.AUTHENTICATED_USER).set(firstUser);
        secondChannel.attr(StateKey.AUTHENTICATED_USER).set(secondUser);

        final FullHttpRequest firstRequest = createRequest("first");
        final FullHttpRequest secondRequest = createRequest("second");
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<Boolean> firstResult = executor.submit(() -> firstChannel.writeInbound(firstRequest));
            assertTrue(firstRequestInAuthorizer.await(5, TimeUnit.SECONDS));

            secondChannel.writeInbound(secondRequest);
            continueFirstRequest.countDown();
            firstResult.get(5, TimeUnit.SECONDS);

            assertEquals(1, firstUser.getNameCalls());
            assertEquals(0, secondUser.getNameCalls());
        } finally {
            continueFirstRequest.countDown();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            firstChannel.finishAndReleaseAll();
            secondChannel.finishAndReleaseAll();
        }
    }

    private static FullHttpRequest createRequest(final String script) {
        final QueryStringEncoder encoder = new QueryStringEncoder("/");
        encoder.addParam(Tokens.ARGS_GREMLIN, script);
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, encoder.toString());
    }

    private static class BlockingAuthorizer implements Authorizer {
        private final CountDownLatch firstRequestInAuthorizer;
        private final CountDownLatch continueFirstRequest;

        private BlockingAuthorizer(final CountDownLatch firstRequestInAuthorizer,
                                   final CountDownLatch continueFirstRequest) {
            this.firstRequestInAuthorizer = firstRequestInAuthorizer;
            this.continueFirstRequest = continueFirstRequest;
        }

        @Override
        public void setup(final Map<String, Object> config) {
        }

        @Override
        public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode,
                                  final Map<String, String> aliases) throws AuthorizationException {
            return bytecode;
        }

        @Override
        public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
            if (!"first".equals(msg.getArg(Tokens.ARGS_GREMLIN)))
                return;

            firstRequestInAuthorizer.countDown();
            try {
                if (!continueFirstRequest.await(5, TimeUnit.SECONDS))
                    throw new AuthorizationException("Timed out waiting for the second request");
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AuthorizationException("Interrupted while waiting for the second request", ex);
            }
            throw new AuthorizationException("Request rejected");
        }
    }

    private static class RecordingUser extends AuthenticatedUser {
        private final AtomicInteger nameCalls = new AtomicInteger();

        private RecordingUser(final String name) {
            super(name);
        }

        @Override
        public String getName() {
            nameCalls.incrementAndGet();
            return super.getName();
        }

        private int getNameCalls() {
            return nameCalls.get();
        }
    }

    @Test
    public void shouldReleaseRejectedGraphBinaryRequest() throws Exception {
        final RequestMessage requestMessage = RequestMessage.build("eval").addArg("gremlin", "g.V()").create();
        final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/",
                new GraphBinaryMessageSerializerV1().serializeRequestAsBinary(requestMessage, ByteBufAllocator.DEFAULT));
        final EmbeddedChannel channel = new EmbeddedChannel(new HttpBasicAuthorizationHandler(authorizer));
        FullHttpResponse response = null;
        try {
            assertFalse(channel.writeInbound(request));

            assertEquals(0, request.refCnt());
            response = channel.readOutbound();
            assertNotNull(response);
            assertEquals(BAD_REQUEST, response.status());
        } finally {
            ReferenceCountUtil.release(response);
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldTransferAuthorizedRequestOwnership() {
        final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        buffer.writeCharSequence("{\"gremlin\":\"g.V()\"}", CharsetUtil.UTF_8);
        final FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.POST, "/", buffer, new DefaultHttpHeaders(), new DefaultHttpHeaders());
        final EmbeddedChannel channel = new EmbeddedChannel(new HttpBasicAuthorizationHandler(authorizer));
        FullHttpRequest forwarded = null;
        try {
            assertTrue(channel.writeInbound(request));

            assertEquals(1, request.refCnt());
            forwarded = channel.readInbound();
            assertSame(request, forwarded);
        } finally {
            ReferenceCountUtil.release(forwarded);
            channel.finishAndReleaseAll();
        }
    }
}
