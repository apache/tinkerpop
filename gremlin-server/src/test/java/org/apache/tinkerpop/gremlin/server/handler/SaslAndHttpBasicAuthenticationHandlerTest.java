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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.apache.tinkerpop.gremlin.server.AbstractChannelizer.PIPELINE_AUTHENTICATOR;
import static org.apache.tinkerpop.gremlin.server.AbstractChannelizer.PIPELINE_AUTHORIZER;
import static org.apache.tinkerpop.gremlin.server.AbstractChannelizer.PIPELINE_REQUEST_HANDLER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SaslAndHttpBasicAuthenticationHandlerTest {

    private static final Map<String, MessageSerializer<?>> NO_SERIALIZERS = Collections.emptyMap();

    /**
     * {@link HttpGremlinEndpointHandler} authorizes the request itself, so no {@link HttpBasicAuthorizationHandler}
     * is added and the request is authorized once. This holds under the name {@code HttpChannelizer} gives the
     * handler as well as the one {@code WsAndHttpChannelizer} and {@code UnifiedChannelizer} give it.
     */
    @Test
    public void shouldNotAddHttpBasicAuthorizationHandlerInFrontOfEndpointHandler() {
        for (final String name : new String[] {PIPELINE_REQUEST_HANDLER, "http-gremlin-handler"}) {
            final CountingAuthorizer authorizer = new CountingAuthorizer();
            final Settings settings = new Settings();
            settings.authorization.overrideAuthorizer(authorizer);

            final EmbeddedChannel channel = channel(authorizer, name,
                    new HttpGremlinEndpointHandler(NO_SERIALIZERS, null, null, settings));
            try {
                assertUnauthorized(channel);
                assertNull(name, channel.pipeline().get(PIPELINE_AUTHORIZER));
                assertEquals(name, 1, authorizer.calls.get());
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    /**
     * A request handler other than {@link HttpGremlinEndpointHandler} keeps the
     * {@link HttpBasicAuthorizationHandler} in front of it, so its requests are still authorized.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void shouldAddHttpBasicAuthorizationHandlerInFrontOfOtherRequestHandler() {
        final CountingAuthorizer authorizer = new CountingAuthorizer();

        final EmbeddedChannel channel = channel(authorizer, PIPELINE_REQUEST_HANDLER, new ChannelInboundHandlerAdapter());
        try {
            assertUnauthorized(channel);
            assertThat(channel.pipeline().get(PIPELINE_AUTHORIZER), instanceOf(HttpBasicAuthorizationHandler.class));
            assertEquals(1, authorizer.calls.get());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /**
     * A subclass of {@link HttpGremlinEndpointHandler} that keeps the inherited {@code channelRead()} authorizes
     * requests itself, so no {@link HttpBasicAuthorizationHandler} is added and the request is authorized once.
     */
    @Test
    public void shouldNotAddHttpBasicAuthorizationHandlerInFrontOfEndpointSubclassKeepingChannelRead() {
        final CountingAuthorizer authorizer = new CountingAuthorizer();
        final Settings settings = new Settings();
        settings.authorization.overrideAuthorizer(authorizer);
        final HttpGremlinEndpointHandler endpoint = new HttpGremlinEndpointHandler(NO_SERIALIZERS, null, null, settings) {
        };

        final EmbeddedChannel channel = channel(authorizer, PIPELINE_REQUEST_HANDLER, endpoint);
        try {
            assertUnauthorized(channel);
            assertNull(channel.pipeline().get(PIPELINE_AUTHORIZER));
            assertEquals(1, authorizer.calls.get());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /**
     * A subclass of {@link HttpGremlinEndpointHandler} that overrides {@code channelRead()} does not authorize
     * requests itself, so the {@link HttpBasicAuthorizationHandler} is added in front of it.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void shouldAddHttpBasicAuthorizationHandlerInFrontOfEndpointSubclassOverridingChannelRead() {
        final CountingAuthorizer authorizer = new CountingAuthorizer();
        final Settings settings = new Settings();
        settings.authorization.overrideAuthorizer(authorizer);
        final HttpGremlinEndpointHandler endpoint = new HttpGremlinEndpointHandler(NO_SERIALIZERS, null, null, settings) {
            @Override
            public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
                ReferenceCountUtil.release(msg);
            }
        };

        final EmbeddedChannel channel = channel(authorizer, PIPELINE_REQUEST_HANDLER, endpoint);
        try {
            assertUnauthorized(channel);
            assertThat(channel.pipeline().get(PIPELINE_AUTHORIZER), instanceOf(HttpBasicAuthorizationHandler.class));
            assertEquals(1, authorizer.calls.get());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /**
     * An {@link HttpGremlinEndpointHandler} built from settings that name an authorizer but hold no instance does not
     * authorize requests itself, so the {@link HttpBasicAuthorizationHandler} is added in front of it.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void shouldAddHttpBasicAuthorizationHandlerInFrontOfEndpointWithoutAuthorizerInstance() {
        final CountingAuthorizer authorizer = new CountingAuthorizer();
        final Settings settings = new Settings();
        settings.authorization.authorizer = CountingAuthorizer.class.getName();

        final EmbeddedChannel channel = channel(authorizer, PIPELINE_REQUEST_HANDLER,
                new HttpGremlinEndpointHandler(NO_SERIALIZERS, null, null, settings));
        try {
            assertUnauthorized(channel);
            assertThat(channel.pipeline().get(PIPELINE_AUTHORIZER), instanceOf(HttpBasicAuthorizationHandler.class));
            assertEquals(1, authorizer.calls.get());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static EmbeddedChannel channel(final Authorizer authorizer, final String requestHandlerName,
                                           final ChannelHandler requestHandler) {
        final EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(PIPELINE_AUTHENTICATOR,
                new SaslAndHttpBasicAuthenticationHandler(new NamedUserAuthenticator(), authorizer, new Settings()));
        channel.pipeline().addLast(requestHandlerName, requestHandler);
        return channel;
    }

    private static void assertUnauthorized(final EmbeddedChannel channel) {
        final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/?gremlin=1");
        request.headers().add(HttpHeaderNames.AUTHORIZATION, "Basic " +
                Base64.getUrlEncoder().encodeToString("stephen:password".getBytes(StandardCharsets.UTF_8)));
        FullHttpResponse response = null;
        try {
            channel.writeInbound(request);
            response = channel.readOutbound();
            assertNotNull(response);
            assertEquals(UNAUTHORIZED, response.status());
        } finally {
            ReferenceCountUtil.release(response);
        }
    }

    private static class NamedUserAuthenticator implements Authenticator {
        @Override
        public boolean requireAuthentication() {
            return true;
        }

        @Override
        public void setup(final Map<String, Object> config) {
        }

        @Override
        public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AuthenticatedUser authenticate(final Map<String, String> credentials) {
            return new AuthenticatedUser("stephen");
        }
    }

    private static class CountingAuthorizer implements Authorizer {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public void setup(final Map<String, Object> config) {
        }

        @Override
        public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode,
                                  final Map<String, String> aliases) throws AuthorizationException {
            calls.incrementAndGet();
            throw new AuthorizationException("denied");
        }

        @Override
        public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
            calls.incrementAndGet();
            throw new AuthorizationException("denied");
        }
    }
}
