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
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.tinkerpop.gremlin.driver.HttpRequest;
import org.apache.tinkerpop.gremlin.driver.RequestInterceptor;
import org.apache.tinkerpop.gremlin.driver.UserAgent;
import org.apache.tinkerpop.gremlin.driver.auth.Auth;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link HttpGremlinRequestEncoder} covering the request-rejection, header-population, transaction-id
 * promotion, interceptor-failure and unconnected-channel branches. Uses {@link EmbeddedChannel} to push
 * {@link RequestMessage} instances through the encoder and inspect the produced {@link FullHttpRequest}.
 */
public class HttpGremlinRequestEncoderTest {

    private final MessageSerializer<?> serializer = Serializers.GRAPHBINARY_V4.simpleInstance();
    private final URI uri = URI.create("http://localhost:8182/gremlin");

    // A resolved loopback address so getRemoteAddress().getAddress().getHostAddress() works (createUnresolved would
    // yield a null InetAddress).
    private static final SocketAddress RESOLVED_REMOTE = new InetSocketAddress(InetAddress.getLoopbackAddress(), 8182);

    // EmbeddedChannel's default remoteAddress is an EmbeddedSocketAddress which the encoder cannot cast to
    // InetSocketAddress, so override remoteAddress0() to return exactly what the test needs (or null to simulate an
    // unconnected channel).
    private static EmbeddedChannel channelWith(final SocketAddress remote, final HttpGremlinRequestEncoder encoder) {
        return new EmbeddedChannel(encoder) {
            @Override
            protected SocketAddress remoteAddress0() {
                return remote;
            }
        };
    }

    private EmbeddedChannel connectedChannel(final HttpGremlinRequestEncoder encoder) {
        return channelWith(RESOLVED_REMOTE, encoder);
    }

    private HttpGremlinRequestEncoder encoder(final List<RequestInterceptor> interceptors, final boolean userAgentEnabled,
                                              final boolean bulkResults, final boolean compressionEnabled) {
        return new HttpGremlinRequestEncoder(serializer, interceptors, userAgentEnabled, bulkResults, compressionEnabled, uri);
    }

    private static <T extends Throwable> T findCause(final Throwable thrown, final Class<T> type) {
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return type.cast(t);
            }
        }
        return null;
    }

    /**
     * A {@link RequestMessage} whose {@code gremlin} field holds a {@link GremlinLang} instance cannot be sent over
     * HTTP and must be rejected before any serialization occurs.
     */
    @Test
    public void shouldRejectGremlinLangGremlinField() throws Exception {
        final EmbeddedChannel channel = connectedChannel(encoder(Collections.emptyList(), false, false, false));
        final RequestMessage request = RequestMessage.build("unused")
                .add("gremlin", new GremlinLang())
                .create();

        final Throwable thrown = assertThrows(Throwable.class, () -> channel.writeOutbound(request));

        final ResponseException cause = findCause(thrown, ResponseException.class);
        assertNotNull("Expected a ResponseException in the cause chain of " + thrown, cause);
        assertTrue("ResponseException message should mention GremlinLang: " + cause.getMessage(),
                cause.getMessage().contains("GremlinLang"));
    }

    /**
     * With user-agent, compression and bulk-results all enabled, the produced request must carry the HOST, ACCEPT,
     * ACCEPT_ENCODING (deflate), USER_AGENT and bulk-results headers.
     */
    @Test
    public void shouldPopulateOptionalHeadersWhenFlagsEnabled() throws Exception {
        final EmbeddedChannel channel = connectedChannel(encoder(Collections.emptyList(), true, true, true));
        final RequestMessage request = RequestMessage.build("g.V()").create();

        assertTrue(channel.writeOutbound(request));
        final FullHttpRequest produced = channel.readOutbound();
        assertNotNull(produced);

        assertNotNull("HOST header should be present", produced.headers().get(HttpRequest.Headers.HOST));
        assertEquals(serializer.mimeTypesSupported()[0], produced.headers().get(HttpRequest.Headers.ACCEPT));
        assertEquals(HttpRequest.Headers.DEFLATE, produced.headers().get(HttpRequest.Headers.ACCEPT_ENCODING));
        assertEquals(UserAgent.USER_AGENT, produced.headers().get(HttpRequest.Headers.USER_AGENT));
        assertEquals("true", produced.headers().get(Tokens.BULK_RESULTS));

        produced.release();
    }

    /**
     * With user-agent, compression and bulk-results all disabled, the corresponding optional headers must be absent
     * while the mandatory HOST and ACCEPT headers remain.
     */
    @Test
    public void shouldOmitOptionalHeadersWhenFlagsDisabled() throws Exception {
        final EmbeddedChannel channel = connectedChannel(encoder(Collections.emptyList(), false, false, false));
        final RequestMessage request = RequestMessage.build("g.V()").create();

        assertTrue(channel.writeOutbound(request));
        final FullHttpRequest produced = channel.readOutbound();
        assertNotNull(produced);

        assertNotNull("HOST header should be present", produced.headers().get(HttpRequest.Headers.HOST));
        assertEquals(serializer.mimeTypesSupported()[0], produced.headers().get(HttpRequest.Headers.ACCEPT));
        assertFalse("ACCEPT_ENCODING should be absent", produced.headers().contains(HttpRequest.Headers.ACCEPT_ENCODING));
        assertFalse("USER_AGENT should be absent", produced.headers().contains(HttpRequest.Headers.USER_AGENT));
        assertFalse("bulk-results header should be absent", produced.headers().contains(Tokens.BULK_RESULTS));

        produced.release();
    }

    /**
     * A transaction id carried in the request fields must be promoted to the {@code X-Transaction-Id} HTTP header.
     */
    @Test
    public void shouldPromoteTransactionIdToHeader() throws Exception {
        final EmbeddedChannel channel = connectedChannel(encoder(Collections.emptyList(), false, false, false));
        final String transactionId = "d3b07384-d9a0-4c9b-8f4a-000000000001";
        final RequestMessage request = RequestMessage.build("g.tx().commit()")
                .add(Tokens.ARGS_TRANSACTION_ID, transactionId)
                .create();

        assertTrue(channel.writeOutbound(request));
        final FullHttpRequest produced = channel.readOutbound();
        assertNotNull(produced);

        assertEquals(transactionId, produced.headers().get(Tokens.Headers.TRANSACTION_ID));

        produced.release();
    }

    /**
     * An interceptor that fails authentication must cause the encode to surface a {@link ResponseException} whose
     * message identifies the failure as an authentication error.
     */
    @Test
    public void shouldWrapAuthenticationExceptionAsResponseException() throws Exception {
        final RequestInterceptor failingInterceptor = request -> {
            throw new Auth.AuthenticationException(new IOException("bad credentials"));
        };
        final EmbeddedChannel channel = connectedChannel(
                encoder(Collections.singletonList(failingInterceptor), false, false, false));
        final RequestMessage request = RequestMessage.build("g.V()").create();

        final Throwable thrown = assertThrows(Throwable.class, () -> channel.writeOutbound(request));

        final ResponseException cause = findCause(thrown, ResponseException.class);
        assertNotNull("Expected a ResponseException in the cause chain of " + thrown, cause);
        assertTrue("ResponseException message should mention authentication: " + cause.getMessage(),
                cause.getMessage().contains("authentication"));
    }

    /**
     * When the channel is not connected (null remote address) and no inbound SSL exception is recorded, encoding must
     * fail with a {@link RuntimeException} explaining that the channel is not connected.
     */
    @Test
    public void shouldFailWhenChannelNotConnected() {
        // remoteAddress0() returns null to simulate an unconnected channel.
        final EmbeddedChannel channel = channelWith(null, encoder(Collections.emptyList(), false, false, false));
        final RequestMessage request = RequestMessage.build("g.V()").create();

        final Throwable thrown = assertThrows(Throwable.class, () -> channel.writeOutbound(request));

        final RuntimeException cause = findCause(thrown, RuntimeException.class);
        assertNotNull("Expected a RuntimeException in the cause chain of " + thrown, cause);
        assertTrue("Message should mention that the channel is not connected: " + cause.getMessage(),
                cause.getMessage().contains("not connected"));
    }

    /**
     * When the channel is not connected because of a recorded inbound SSL exception, encoding must fail with a
     * {@link RuntimeException} that attributes the failure to the ssl error.
     */
    @Test
    public void shouldFailWithSslReasonWhenSslExceptionRecorded() {
        final EmbeddedChannel channel = channelWith(null, encoder(Collections.emptyList(), false, false, false));
        channel.attr(GremlinResponseHandler.INBOUND_SSL_EXCEPTION).set(new RuntimeException("handshake failed"));
        final RequestMessage request = RequestMessage.build("g.V()").create();

        final Throwable thrown = assertThrows(Throwable.class, () -> channel.writeOutbound(request));

        final RuntimeException cause = findCause(thrown, RuntimeException.class);
        assertNotNull("Expected a RuntimeException in the cause chain of " + thrown, cause);
        assertTrue("Message should attribute the failure to an ssl error: " + cause.getMessage(),
                cause.getMessage().contains("ssl"));
    }
}
