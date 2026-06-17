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

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.tinkerpop.gremlin.driver.Channelizer.HttpChannelizer.LAST_CONTENT_READ_RESPONSE;
import static org.junit.Assert.*;

public class HttpStreamingResponseHandlerTest {

    private ExecutorService executor;
    private GraphBinaryMessageSerializerV4 serializer;
    private GraphBinaryReader reader;

    @Before
    public void setup() {
        executor = Executors.newSingleThreadExecutor();
        serializer = new GraphBinaryMessageSerializerV4();
        reader = serializer.getMapper().getReader();
    }

    @After
    public void teardown() {
        executor.shutdownNow();
    }

    private EmbeddedChannel createChannel(final AtomicReference<ResultSet> pendingResultSet) {
        final HttpStreamingResponseHandler handler = new HttpStreamingResponseHandler(
                reader, pendingResultSet, executor);
        return new EmbeddedChannel(handler);
    }

    @Test
    public void shouldEmitLastContentReadResponseOnHappyPath() throws Exception {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);
        final EmbeddedChannel channel = createChannel(pending);

        // Serialize a valid GraphBinary response
        final byte[] payload = toBytes(serializer.serializeResponseAsBinary(
                ResponseMessage.build().code(HttpResponseStatus.OK)
                        .result(Collections.singletonList(1)).create(),
                channel.alloc()));

        // Send HttpResponse with GraphBinary content type
        final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V4);
        channel.writeInbound(response);

        // Send content
        channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(payload)));

        // Send LastHttpContent
        channel.writeInbound(new DefaultLastHttpContent());

        // Verify LAST_CONTENT_READ_RESPONSE is emitted
        final Object out = channel.readInbound();
        assertSame(LAST_CONTENT_READ_RESPONSE, out);

        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldHandleDoubleLastHttpContentWithoutError() throws Exception {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);
        final EmbeddedChannel channel = createChannel(pending);

        final byte[] payload = toBytes(serializer.serializeResponseAsBinary(
                ResponseMessage.build().code(HttpResponseStatus.OK)
                        .result(Collections.singletonList(1)).create(),
                channel.alloc()));

        final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V4);
        channel.writeInbound(response);
        channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(payload)));
        channel.writeInbound(new DefaultLastHttpContent());

        // Send a second LastHttpContent — should not throw NPE
        channel.writeInbound(new DefaultLastHttpContent());

        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldSignalQueueInputStreamOnChannelInactive() throws Exception {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);
        final EmbeddedChannel channel = createChannel(pending);

        final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V4);
        channel.writeInbound(response);

        // Send some content but no LastHttpContent
        channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[]{1, 2, 3})));

        // Fire channelInactive — should not throw and should signal the stream
        channel.pipeline().fireChannelInactive();

        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldMarkErrorOnResultSetForNonGraphBinaryError() throws Exception {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);
        final EmbeddedChannel channel = createChannel(pending);

        // Send a 500 response with JSON content type
        final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        channel.writeInbound(response);

        // Send JSON error body
        final String errorJson = "{\"message\":\"test error\"}";
        channel.writeInbound(new DefaultLastHttpContent(Unpooled.copiedBuffer(errorJson, CharsetUtil.UTF_8)));

        // Verify error is marked on the ResultSet
        try {
            rs.all().get();
            fail("Expected exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ResponseException);
            final ResponseException re = (ResponseException) e.getCause();
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
            assertEquals("test error", re.getMessage());
        }

        // Verify pendingResultSet was cleared
        assertNull(pending.get());

        channel.finishAndReleaseAll();
    }

    private byte[] toBytes(final io.netty.buffer.ByteBuf buf) {
        final byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        buf.release();
        return bytes;
    }
}
