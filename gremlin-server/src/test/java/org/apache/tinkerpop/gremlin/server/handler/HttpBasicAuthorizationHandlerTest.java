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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringEncoder;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HttpBasicAuthorizationHandlerTest {

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
}
