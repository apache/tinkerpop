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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.Test;

import java.util.Collections;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.tinkerpop.gremlin.driver.Channelizer.HttpChannelizer.LAST_CONTENT_READ_RESPONSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link HttpGremlinResponseDecoder} exercising the error-status, content-type and error-handling
 * branches that are not covered by the round-trip serialization path. Uses {@link EmbeddedChannel} to push
 * {@link FullHttpResponse} instances directly through the decoder.
 */
public class HttpGremlinResponseDecoderTest {

    private final MessageSerializer<?> serializer = Serializers.GRAPHBINARY_V4.simpleInstance();

    /**
     * Error status with a non-serializer content type is treated as a JSON error body. When a {@code message} field
     * is present it should be surfaced as the status message.
     */
    @Test
    public void shouldDecodeErrorResponseUsingJsonMessage() {
        final EmbeddedChannel testChannel = initializeChannel();
        final FullHttpResponse response = jsonResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "{\"message\":\"something failed\"}");

        testChannel.writeInbound(response);

        final ResponseMessage decoded = testChannel.readInbound();
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, decoded.getStatus().getCode());
        assertEquals("something failed", decoded.getStatus().getMessage());
    }

    /**
     * Error status JSON body without a {@code message} field should fall back to the status reason phrase.
     */
    @Test
    public void shouldFallBackToReasonPhraseWhenJsonHasNoMessage() {
        final EmbeddedChannel testChannel = initializeChannel();
        final FullHttpResponse response = jsonResponse(HttpResponseStatus.BAD_REQUEST, "{\"other\":\"value\"}");

        testChannel.writeInbound(response);

        final ResponseMessage decoded = testChannel.readInbound();
        assertEquals(HttpResponseStatus.BAD_REQUEST, decoded.getStatus().getCode());
        assertEquals(HttpResponseStatus.BAD_REQUEST.reasonPhrase(), decoded.getStatus().getMessage());
    }

    /**
     * A present-but-empty {@code message} field should also fall back to the reason phrase (covers the
     * {@code message.isEmpty()} branch of the ternary).
     */
    @Test
    public void shouldFallBackToReasonPhraseWhenJsonMessageIsEmpty() {
        final EmbeddedChannel testChannel = initializeChannel();
        final FullHttpResponse response = jsonResponse(HttpResponseStatus.NOT_FOUND, "{\"message\":\"\"}");

        testChannel.writeInbound(response);

        final ResponseMessage decoded = testChannel.readInbound();
        assertEquals(HttpResponseStatus.NOT_FOUND, decoded.getStatus().getCode());
        assertEquals(HttpResponseStatus.NOT_FOUND.reasonPhrase(), decoded.getStatus().getMessage());
    }

    /**
     * Even when the status is an error, if the content type matches the serializer's mime type the body must be
     * deserialized as a binary response (the error footer carries the code), not parsed as JSON.
     */
    @Test
    public void shouldDeserializeBinaryWhenErrorStatusHasSerializerContentType() throws SerializationException {
        final EmbeddedChannel testChannel = initializeChannel();
        final ResponseMessage errorResponse = ResponseMessage.build()
                .code(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                .statusMessage("boom")
                .result(Collections.emptyList())
                .create();
        final ByteBuf buffer = serializer.serializeResponseAsBinary(errorResponse, ByteBufAllocator.DEFAULT);
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V4);
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
                HttpResponseStatus.INTERNAL_SERVER_ERROR, buffer, headers, new DefaultHttpHeaders());

        testChannel.writeInbound(response);

        final ResponseMessage decoded = testChannel.readInbound();
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, decoded.getStatus().getCode());
        assertEquals("boom", decoded.getStatus().getMessage());
    }

    /**
     * A malformed binary body (first byte lacking the required most-significant bit) causes the serializer to throw
     * a {@link SerializationException}, which the decoder must wrap in a {@link RuntimeException}.
     */
    @Test
    public void shouldWrapSerializationExceptionInRuntimeException() {
        final EmbeddedChannel testChannel = initializeChannel();
        // OK status routes to deserializeBinaryResponse; 0x00 has its MSB unset so GraphBinary rejects it.
        final ByteBuf content = Unpooled.wrappedBuffer(new byte[]{0x00, 0x01, 0x02});
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK, content,
                new DefaultHttpHeaders(), new DefaultHttpHeaders());

        try {
            testChannel.writeInbound(response);
            fail("Expected an exception wrapping a SerializationException.");
        } catch (Exception e) {
            // The meaningful behavior is that the production code surfaces the underlying SerializationException;
            // MessageToMessageDecoder wraps the decoder's failure in a DecoderException, so search the whole chain.
            boolean sawSerialization = false;
            for (Throwable t = e; t != null; t = t.getCause()) {
                sawSerialization |= t instanceof SerializationException;
            }
            assertTrue("Expected a SerializationException in the cause chain of " + e, sawSerialization);
        }
    }

    /**
     * After a successful decode the decoder must emit the decoded {@link ResponseMessage} followed by the
     * {@link org.apache.tinkerpop.gremlin.driver.Channelizer.HttpChannelizer#LAST_CONTENT_READ_RESPONSE} signal, and
     * must record that bytes were read on the channel so the inactivity handler treats the connection as live.
     */
    @Test
    public void shouldEmitLastContentSignalAndMarkBytesRead() throws SerializationException {
        final EmbeddedChannel testChannel = initializeChannel();
        final ResponseMessage ok = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .result(Collections.singletonList("value"))
                .create();
        final ByteBuf buffer = serializer.serializeResponseAsBinary(ok, ByteBufAllocator.DEFAULT);
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK, buffer,
                new DefaultHttpHeaders(), new DefaultHttpHeaders());

        testChannel.writeInbound(response);

        final ResponseMessage decoded = testChannel.readInbound();
        assertEquals("value", decoded.getResult().getData().get(0));

        final ResponseMessage lastContentSignal = testChannel.readInbound();
        assertSame(LAST_CONTENT_READ_RESPONSE, lastContentSignal);

        final Integer bytesRead = testChannel.attr(InactiveChannelHandler.BYTES_READ).get();
        assertNotNull("BYTES_READ should be set so the connection is treated as active", bytesRead);
        assertEquals(Integer.valueOf(0), bytesRead);
    }

    private EmbeddedChannel initializeChannel() {
        return new EmbeddedChannel(new HttpGremlinResponseDecoder(serializer));
    }

    private FullHttpResponse jsonResponse(final HttpResponseStatus status, final String json) {
        final ByteBuf content = Unpooled.copiedBuffer(json, CharsetUtil.UTF_8);
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_TYPE, SerTokens.MIME_JSON);
        return new DefaultFullHttpResponse(HTTP_1_1, status, content, headers, new DefaultHttpHeaders());
    }
}
