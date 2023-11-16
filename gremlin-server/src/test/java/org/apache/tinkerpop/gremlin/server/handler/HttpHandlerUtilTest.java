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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HttpHandlerUtilTest {

    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private final GraphBinaryMessageSerializerV1 graphBinarySerializer = new GraphBinaryMessageSerializerV1();
    public final GraphSONMessageSerializerV3 graphSONSerializer = new GraphSONMessageSerializerV3();

    @Test
    public void shouldFailWhenIncorrectSerializerUsed() throws SerializationException {
        final RequestMessage request = RequestMessage.build(Tokens.OPS_BYTECODE)
                .processor("traversal")
                .overrideRequestId(UUID.randomUUID())
                .addArg(Tokens.ARGS_GREMLIN, "g.V()")
                .create();

        final ByteBuf buffer = graphSONSerializer.serializeRequestAsBinary(request, allocator);

        final HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V1);

        final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "some uri",
                buffer, headers, new DefaultHttpHeaders());

        final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
        serializers.put(SerTokens.MIME_GRAPHBINARY_V1, graphBinarySerializer);

        try {
            HttpHandlerUtil.getRequestMessageFromHttpRequest(httpRequest, serializers);
            fail("SerializationException expected");
        } catch (IllegalArgumentException ex) {
            assertEquals("Mime type mismatch. Value in content-type header is not equal payload header.", ex.getMessage());
        }
    }

    @Test
    public void shouldCorrectlyDeserializeRequestMessage() throws SerializationException {
        final RequestMessage request = RequestMessage.build(Tokens.OPS_BYTECODE)
                .processor("traversal")
                .overrideRequestId(UUID.randomUUID())
                .addArg(Tokens.ARGS_GREMLIN, "g.V()")
                .create();

        final ByteBuf buffer = graphBinarySerializer.serializeRequestAsBinary(request, allocator);

        final HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V1);

        final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "some uri",
                buffer, headers, new DefaultHttpHeaders());

        final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
        serializers.put(SerTokens.MIME_GRAPHBINARY_V1, graphBinarySerializer);

        final RequestMessage deserialized = HttpHandlerUtil.getRequestMessageFromHttpRequest(httpRequest, serializers);
        assertThat(request, samePropertyValuesAs(deserialized));
    }

    @Test
    public void shouldCorrectlyDeserializeGremlinFromPostRequest() throws SerializationException {
        final String gremlin = "g.V().hasLabel('person')";
        final UUID requestId = UUID.randomUUID();
        final ByteBuf buffer = allocator.buffer();
        buffer.writeCharSequence("{ \"gremlin\": \"" + gremlin +
                        "\", \"requestId\": \"" + requestId +
                        "\", \"language\":  \"gremlin-groovy\"}",
                CharsetUtil.UTF_8);

        final HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_JSON);

        final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "some uri",
                buffer, headers, new DefaultHttpHeaders());

        final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
        serializers.put(SerTokens.MIME_GRAPHBINARY_V1, graphBinarySerializer);

        final RequestMessage deserialized = HttpHandlerUtil.getRequestMessageFromHttpRequest(httpRequest, serializers);
        assertEquals(gremlin, deserialized.getArgs().get(Tokens.ARGS_GREMLIN));
        assertEquals(requestId, deserialized.getRequestId());
        assertEquals("gremlin-groovy", deserialized.getArg(Tokens.ARGS_LANGUAGE));
    }

    @Test
    public void shouldCorrectlyDeserializeGremlinFromGetRequest() throws SerializationException {
        final String gremlin = "g.V().hasLabel('person')";
        final UUID requestId = UUID.randomUUID();
        final ByteBuf buffer = allocator.buffer();

        final List<String> headerOptions = Arrays.asList("", null, SerTokens.MIME_JSON, "some invalid value");

        for (final String contentTypeValue : headerOptions) {
            final HttpHeaders headers = new DefaultHttpHeaders();
            if (contentTypeValue != null) {
                headers.add(HttpHeaderNames.CONTENT_TYPE, contentTypeValue);
            }

            final QueryStringEncoder encoder = new QueryStringEncoder("/");
            encoder.addParam("gremlin", gremlin);
            encoder.addParam("requestId", requestId.toString());
            encoder.addParam("language", "gremlin-groovy");

            final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                    encoder.toString(), buffer, headers, new DefaultHttpHeaders());

            final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
            serializers.put(SerTokens.MIME_GRAPHBINARY_V1, graphBinarySerializer);

            final RequestMessage deserialized = HttpHandlerUtil.getRequestMessageFromHttpRequest(httpRequest, serializers);
            assertEquals(gremlin, deserialized.getArgs().get(Tokens.ARGS_GREMLIN));
            assertEquals(requestId, deserialized.getRequestId());
            assertEquals("gremlin-groovy", deserialized.getArg(Tokens.ARGS_LANGUAGE));
        }
    }
}
