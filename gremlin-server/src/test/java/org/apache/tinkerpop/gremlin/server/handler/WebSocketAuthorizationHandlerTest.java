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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WebSocketAuthorizationHandlerTest {

    @Test
    public void shouldAuthorizeEachRequestWithTheUserFromItsChannel() throws Exception {
        final CountDownLatch firstRequestReadUser = new CountDownLatch(1);
        final CountDownLatch continueFirstRequest = new CountDownLatch(1);
        final Bytecode firstBytecode = new Bytecode();
        final Bytecode secondBytecode = new Bytecode();
        final Map<String, String> aliases = Collections.singletonMap("g", "g");
        final BlockingRequestArguments firstArgs =
                new BlockingRequestArguments(firstRequestReadUser, continueFirstRequest);
        firstArgs.put(Tokens.ARGS_GREMLIN, firstBytecode);
        firstArgs.put(Tokens.ARGS_ALIASES, aliases);
        final RequestMessage firstRequest = createRequest(firstArgs);
        final RequestMessage secondRequest = RequestMessage.build(Tokens.OPS_BYTECODE)
                .addArg(Tokens.ARGS_GREMLIN, secondBytecode)
                .addArg(Tokens.ARGS_ALIASES, aliases).create();

        final RecordingAuthorizer authorizer = new RecordingAuthorizer();
        final WebSocketAuthorizationHandler handler = new WebSocketAuthorizationHandler(authorizer);
        final EmbeddedChannel firstChannel = new EmbeddedChannel(handler);
        final EmbeddedChannel secondChannel = new EmbeddedChannel(handler);
        final AuthenticatedUser firstUser = new AuthenticatedUser("first");
        final AuthenticatedUser secondUser = new AuthenticatedUser("second");
        firstChannel.attr(StateKey.AUTHENTICATED_USER).set(firstUser);
        secondChannel.attr(StateKey.AUTHENTICATED_USER).set(secondUser);

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<Boolean> firstResult = executor.submit(() -> firstChannel.writeInbound(firstRequest));
            assertTrue(firstRequestReadUser.await(5, TimeUnit.SECONDS));

            secondChannel.writeInbound(secondRequest);
            continueFirstRequest.countDown();
            firstResult.get(5, TimeUnit.SECONDS);

            assertEquals(firstUser, authorizer.usersByBytecode.get(firstBytecode));
            assertEquals(secondUser, authorizer.usersByBytecode.get(secondBytecode));
        } finally {
            continueFirstRequest.countDown();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            firstChannel.finishAndReleaseAll();
            secondChannel.finishAndReleaseAll();
        }
    }

    private static RequestMessage createRequest(final Map<String, Object> args) throws ReflectiveOperationException {
        final RequestMessage.Builder builder = RequestMessage.build(Tokens.OPS_BYTECODE);
        final Field argsField = RequestMessage.Builder.class.getDeclaredField("args");
        argsField.setAccessible(true);
        argsField.set(builder, args);
        return builder.create();
    }

    private static class RecordingAuthorizer implements Authorizer {
        private final Map<Bytecode, AuthenticatedUser> usersByBytecode =
                Collections.synchronizedMap(new IdentityHashMap<>());

        @Override
        public void setup(final Map<String, Object> config) {
        }

        @Override
        public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode,
                                  final Map<String, String> aliases) throws AuthorizationException {
            usersByBytecode.put(bytecode, user);
            return bytecode;
        }

        @Override
        public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
        }
    }

    private static class BlockingRequestArguments extends HashMap<String, Object> {
        private final CountDownLatch requestReadUser;
        private final CountDownLatch continueRequest;
        private final AtomicBoolean blocked = new AtomicBoolean();

        private BlockingRequestArguments(final CountDownLatch requestReadUser,
                                         final CountDownLatch continueRequest) {
            this.requestReadUser = requestReadUser;
            this.continueRequest = continueRequest;
        }

        @Override
        public Object get(final Object key) {
            if (Tokens.ARGS_GREMLIN.equals(key) && blocked.compareAndSet(false, true)) {
                requestReadUser.countDown();
                try {
                    if (!continueRequest.await(5, TimeUnit.SECONDS))
                        throw new IllegalStateException("Timed out waiting for the second request");
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for the second request", ex);
                }
            }

            return super.get(key);
        }
    }
}
