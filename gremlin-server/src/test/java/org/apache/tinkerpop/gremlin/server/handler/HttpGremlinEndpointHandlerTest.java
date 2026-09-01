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
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class HttpGremlinEndpointHandlerTest {

    @Test
    public void shouldReleaseRequestWithEmptySerializedBody() {
        assertMalformedSerializedRequestReleased(new byte[0]);
    }

    @Test
    public void shouldReleaseRequestWithUnsignedMimeTypeLength() {
        final byte[] payload = new byte[129];
        payload[0] = (byte) 0x80;

        assertMalformedSerializedRequestReleased(payload);
    }

    @Test
    public void shouldReleaseRequestWithTruncatedMimeTypeHeader() {
        assertMalformedSerializedRequestReleased(new byte[] { 5, 'a', 'b' });
    }

    @Test
    public void shouldReleaseRequestWhenRuntimeExceptionIsThrown() {
        final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        buffer.writeCharSequence("{\"gremlin\":\"g.V()\"}", CharsetUtil.UTF_8);

        final DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, "application/json");
        headers.add(HttpHeaderNames.ACCEPT, "application/json;q=invalid");
        final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/",
                buffer, headers, new DefaultHttpHeaders());
        final HttpGremlinEndpointHandler handler = new HttpGremlinEndpointHandler(
                Collections.emptyMap(), null, null, null);

        try {
            handler.channelRead(null, request);
            fail("NumberFormatException expected");
        } catch (NumberFormatException ex) {
            assertEquals("For input string: \"invalid\"", ex.getMessage());
        }

        assertEquals(0, request.refCnt());
    }

    private void assertMalformedSerializedRequestReleased(final byte[] payload) {
        final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(payload.length);
        buffer.writeBytes(payload);

        final DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V1);
        final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/",
                buffer, headers, new DefaultHttpHeaders());

        final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
        serializers.put(SerTokens.MIME_GRAPHBINARY_V1, new GraphBinaryMessageSerializerV1());
        final EmbeddedChannel channel = new EmbeddedChannel(
                new HttpGremlinEndpointHandler(serializers, null, null, null));
        FullHttpResponse response = null;
        try {
            channel.writeInbound(request);

            assertEquals(0, request.refCnt());
            response = channel.readOutbound();
            assertNotNull(response);
            assertEquals(BAD_REQUEST, response.status());
        } finally {
            ReferenceCountUtil.release(response);
            channel.finishAndReleaseAll();
        }
    }
}
