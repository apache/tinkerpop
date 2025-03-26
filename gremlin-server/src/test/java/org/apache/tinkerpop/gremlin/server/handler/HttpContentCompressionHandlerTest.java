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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.Inflater;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderValues.DEFLATE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.http.LastHttpContent.EMPTY_LAST_CONTENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HttpContentCompressionHandlerTest {
    @Test
    public void shouldNotCompressIfResponseDoesNotContainContentEncoding() {
        final HttpContentCompressionHandler compressionHandler = new HttpContentCompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(compressionHandler);
        final ByteBuf content = testChannel.alloc().buffer();
        content.writeCharSequence("abc", CharsetUtil.UTF_8);
        final HttpHeaders headers = new DefaultHttpHeaders();
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, content, headers, headers);

        testChannel.writeOutbound(response);

        final FullHttpResponse actualResponse = testChannel.readOutbound();

        assertFalse(actualResponse.headers().contains(CONTENT_ENCODING));
        assertEquals("abc", actualResponse.content().toString(CharsetUtil.UTF_8));
    }

    @Test
    public void shouldCompressIfResponseDoesContainContentEncoding() {
        final HttpContentCompressionHandler compressionHandler = new HttpContentCompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(compressionHandler);
        final ByteBuf content = testChannel.alloc().buffer();
        content.writeCharSequence("aaa", CharsetUtil.UTF_8);
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, content, headers, new DefaultHttpHeaders());

        testChannel.writeOutbound(response);

        final FullHttpResponse actualResponse = testChannel.readOutbound();
        assertTrue(actualResponse.headers().contains(CONTENT_ENCODING));

        final byte[] contentBytes = new byte[actualResponse.content().readableBytes()];
        actualResponse.content().readBytes(contentBytes);
        assertArrayEquals(new byte[] {0x78, (byte) 0x9c, 0x4b, 0x4c, 0x4c, 0x4, 0x0, 0x2, 0x49, 0x1, 0x24}, contentBytes);
    }

    @Test
    public void shouldCompressLargeChunk() throws Exception {
        final int largeArraySize = 64 * 1024 * 1024;
        byte[] largeArray = new byte[largeArraySize];
        Arrays.fill(largeArray, (byte) 0x65);

        final HttpContentCompressionHandler compressionHandler = new HttpContentCompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(compressionHandler);
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final FullHttpResponse outbound = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(largeArray), headers, new DefaultHttpHeaders());

        testChannel.writeOutbound(outbound);

        final FullHttpResponse response = testChannel.readOutbound();
        assertTrue(response.headers().contains(CONTENT_ENCODING));
        final byte[] contentBytes = new byte[response.content().readableBytes()];
        response.content().readBytes(contentBytes);
        response.release();

        final Inflater decompressor = new Inflater();
        decompressor.setInput(contentBytes);
        final byte[] inflatedBytes = new byte[largeArraySize];
        decompressor.inflate(inflatedBytes);

        assertTrue(decompressor.finished());
        decompressor.end();

        assertArrayEquals(largeArray, inflatedBytes);
    }

    @Test
    public void shouldCompressContentInMultipleChunks() throws Exception {
        final HttpContentCompressionHandler compressionHandler = new HttpContentCompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(compressionHandler);

        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK, headers);

        final ByteBuf firstChunk = testChannel.alloc().buffer();
        firstChunk.writeCharSequence("aaa", CharsetUtil.UTF_8);

        final ByteBuf secondChunk = testChannel.alloc().buffer();
        secondChunk.writeCharSequence("bbb", CharsetUtil.UTF_8);

        final ByteBuf thirdChunk = testChannel.alloc().buffer();
        thirdChunk.writeCharSequence("ccc", CharsetUtil.UTF_8);

        testChannel.writeOutbound(response);
        testChannel.writeOutbound(new DefaultHttpContent(firstChunk));
        testChannel.writeOutbound(new DefaultHttpContent(secondChunk));
        testChannel.writeOutbound(new DefaultHttpContent(thirdChunk));
        testChannel.writeOutbound(EMPTY_LAST_CONTENT);

        assertEquals(5, testChannel.outboundMessages().size());

        testChannel.readOutbound(); // Discard HttpResponse.

        Inflater decompressor = new Inflater();
        final List<byte[]> decompressedChunks = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            HttpContent content = testChannel.readOutbound();
            decompressor.setInput(ByteBufUtil.getBytes(content.content()));
            byte[] inflatedBytes = new byte[3];
            decompressor.inflate(inflatedBytes);
            decompressedChunks.add(inflatedBytes);
        }

        assertEquals("aaa", new String(decompressedChunks.get(0), StandardCharsets.UTF_8));
        assertEquals("bbb", new String(decompressedChunks.get(1), StandardCharsets.UTF_8));
        assertEquals("ccc", new String(decompressedChunks.get(2), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldHandleEmptyFrameCompression() throws Exception {
        final HttpContentCompressionHandler compressionHandler = new HttpContentCompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(compressionHandler);
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final FullHttpResponse out = new DefaultFullHttpResponse(HTTP_1_1, OK, EMPTY_BUFFER, headers, new DefaultHttpHeaders());

        testChannel.writeOutbound(out);

        final FullHttpResponse response = testChannel.readOutbound();
        assertTrue(response.headers().contains(CONTENT_ENCODING));
        assertTrue(response.content().readableBytes() > 0);
        // Really just testing to make sure no exceptions occur.
    }

    @Test
    public void shouldHandleUncompressableChunk() throws Exception {
        final int arraySize = 128;
        byte[] data = new byte[arraySize];
        new Random().nextBytes(data);

        final HttpContentCompressionHandler compressionHandler = new HttpContentCompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(compressionHandler);
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final FullHttpResponse outbound = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(data), headers, new DefaultHttpHeaders());

        testChannel.writeOutbound(outbound);

        final FullHttpResponse response = testChannel.readOutbound();
        assertTrue(response.headers().contains(CONTENT_ENCODING));
        final byte[] contentBytes = new byte[response.content().readableBytes()];
        response.content().readBytes(contentBytes);
        response.release();

        final Inflater decompressor = new Inflater();
        decompressor.setInput(contentBytes);
        byte[] inflatedBytes = new byte[arraySize];
        decompressor.inflate(inflatedBytes);

        assertTrue(decompressor.finished());
        decompressor.end();

        assertArrayEquals(data, inflatedBytes);
    }
}
