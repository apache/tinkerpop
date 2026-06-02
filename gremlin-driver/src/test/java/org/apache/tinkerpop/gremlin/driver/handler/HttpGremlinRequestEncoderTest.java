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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.RequestInterceptor;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.UserAgent;
import org.apache.tinkerpop.gremlin.driver.interceptor.PayloadSerializingInterceptor;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HttpGremlinRequestEncoderTest {

    private static final InetSocketAddress REMOTE = new InetSocketAddress("127.0.0.1", 8182);

    private static class TestChannel extends EmbeddedChannel {
        TestChannel(ChannelHandler... handlers) {
            super(handlers);
        }

        @Override
        public SocketAddress remoteAddress() {
            return REMOTE;
        }
    }

    private EmbeddedChannel createChannel(final boolean userAgentEnabled) {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        final List<Pair<String, ? extends RequestInterceptor>> interceptors =
                Collections.singletonList(Pair.of("serializer", new PayloadSerializingInterceptor(serializer)));
        final URI uri = URI.create("http://localhost:8182/gremlin");
        final HttpGremlinRequestEncoder encoder = new HttpGremlinRequestEncoder(
                serializer, interceptors, userAgentEnabled, false, uri);
        return new TestChannel(encoder);
    }

    @Test
    public void shouldIncludeUserAgentHeader() {
        final EmbeddedChannel channel = createChannel(true);
        final RequestMessage msg = RequestMessage.build("g.V()").create();

        assertTrue(channel.writeOutbound(msg));
        final FullHttpRequest request = channel.readOutbound();

        assertNotNull(request);
        assertEquals(UserAgent.USER_AGENT, request.headers().get("user-agent"));
        request.release();
    }

    @Test
    public void shouldExcludeUserAgentHeaderWhenDisabled() {
        final EmbeddedChannel channel = createChannel(false);
        final RequestMessage msg = RequestMessage.build("g.V()").create();

        assertTrue(channel.writeOutbound(msg));
        final FullHttpRequest request = channel.readOutbound();

        assertNotNull(request);
        assertNull(request.headers().get("user-agent"));
        request.release();
    }

    @Test
    public void shouldEncodePerRequestSettingsInBody() throws Exception {
        final EmbeddedChannel channel = createChannel(true);
        final GraphBinaryMessageSerializerV4 deserializer = new GraphBinaryMessageSerializerV4();

        // Build RequestMessage the same way Client.submitAsync does from RequestOptions
        final RequestOptions options = RequestOptions.build()
                .timeout(5000L)
                .batchSize(250)
                .materializeProperties(Tokens.MATERIALIZE_PROPERTIES_TOKENS)
                .create();

        final RequestMessage.Builder builder = RequestMessage.build("g.V()");
        builder.addChunkSize(options.getBatchSize().get());
        options.getTimeout().ifPresent(builder::addTimeoutMillis);
        options.getMaterializeProperties().ifPresent(builder::addMaterializeProperties);
        final RequestMessage msg = builder.create();

        assertTrue(channel.writeOutbound(msg));
        final FullHttpRequest request = channel.readOutbound();
        assertNotNull(request);

        try {
            // Deserialize the body back into a RequestMessage to prove round-trip fidelity
            final ByteBuf body = request.content();
            assertFalse("body should not be empty", body.readableBytes() == 0);
            final RequestMessage decoded = deserializer.deserializeBinaryRequest(body);

            assertEquals(5000L, (long) decoded.getField(Tokens.TIMEOUT_MS));
            assertEquals(250, (int) decoded.getField(Tokens.ARGS_BATCH_SIZE));
            assertEquals(Tokens.MATERIALIZE_PROPERTIES_TOKENS, decoded.getField(Tokens.ARGS_MATERIALIZE_PROPERTIES));
        } finally {
            request.release();
        }
    }
}
