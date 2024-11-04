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
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.buffer.ByteBuf;
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
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderValues.DEFLATE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.http.LastHttpContent.EMPTY_LAST_CONTENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpContentDecompressionHandlerTest {
    @Test
    public void shouldNotDecompressIfResponseDoesNotContainContentEncoding() {
        final HttpContentDecompressionHandler decompressionHandler = new HttpContentDecompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(decompressionHandler);
        final ByteBuf content = testChannel.alloc().buffer();
        content.writeCharSequence("abc", CharsetUtil.UTF_8);
        final HttpHeaders headers = new DefaultHttpHeaders();
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, content, headers, headers);

        testChannel.writeInbound(response);

        final FullHttpResponse inbound = testChannel.readInbound();
        assertEquals("abc", inbound.content().toString(CharsetUtil.UTF_8));
    }

    @Test
    public void shouldDecompressIfResponseDoesContainContentEncoding() {
        final HttpContentDecompressionHandler decompressionHandler = new HttpContentDecompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(decompressionHandler);
        final ByteBuf content = testChannel.alloc().buffer();
        content.writeBytes(new byte[] {0x78, (byte) 0x9c, 0x4b, 0x4c, 0x4c, 0x4, 0x0, 0x2, 0x49, 0x1, 0x24});
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, content, headers, new DefaultHttpHeaders());

        testChannel.writeInbound(response);

        final ByteBuf incomingContent = ((FullHttpResponse) testChannel.readInbound()).content();
        assertTrue("aaa".contentEquals(incomingContent.readCharSequence(incomingContent.readableBytes(), CharsetUtil.UTF_8)));
    }

    @Test
    public void shouldDecompressLargeChunk() throws Exception {
        final int largeArraySize = 64 * 1024 * 1024;
        final int compressedSize = 65239; // pre-calculated compressed size of test data.
        final byte[] largeArray = new byte[largeArraySize];
        Arrays.fill(largeArray, (byte) 0x65);

        final Deflater compressor = new Deflater();
        compressor.setInput(largeArray);
        compressor.finish();
        final byte[] compressedArray = new byte[compressedSize];
        compressor.deflate(compressedArray, 0, compressedArray.length, Deflater.FULL_FLUSH);

        final HttpContentDecompressionHandler decompressionHandler = new HttpContentDecompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(decompressionHandler);
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final FullHttpResponse inbound = new DefaultFullHttpResponse(
                HTTP_1_1, OK, Unpooled.wrappedBuffer(compressedArray, 0, compressedArray.length), headers, new DefaultHttpHeaders());

        testChannel.writeInbound(inbound);

        final FullHttpResponse response = testChannel.readInbound();
        final byte[] contentBytes = new byte[response.content().readableBytes()];
        response.content().readBytes(contentBytes);
        response.release();

        assertArrayEquals(largeArray, contentBytes);
    }

    @Test
    public void shouldDecompressContentInMultipleChunks() throws Exception {
        final HttpContentDecompressionHandler decompressionHandler = new HttpContentDecompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(decompressionHandler);

        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK, headers);

        final Deflater compressor = new Deflater();
        final List<byte[]> decompressedChunks = new ArrayList<>();

        int len;
        final byte[] compressionBuffer = new byte[100];
        compressor.setInput("aaa".getBytes(StandardCharsets.UTF_8));
        len = compressor.deflate(compressionBuffer, 0, compressionBuffer.length, Deflater.SYNC_FLUSH);
        decompressedChunks.add(Arrays.copyOfRange(compressionBuffer, 0, len));

        compressor.setInput("bbb".getBytes(StandardCharsets.UTF_8));
        len = compressor.deflate(compressionBuffer, 0, compressionBuffer.length, Deflater.SYNC_FLUSH);
        decompressedChunks.add(Arrays.copyOfRange(compressionBuffer, 0, len));

        compressor.setInput("ccc".getBytes(StandardCharsets.UTF_8));
        len = compressor.deflate(compressionBuffer, 0, compressionBuffer.length, Deflater.SYNC_FLUSH);
        decompressedChunks.add(Arrays.copyOfRange(compressionBuffer, 0, len));
        compressor.reset();

        testChannel.writeInbound(response);
        for (int i = 0; i < 3; i++) {
            byte[] chunk = decompressedChunks.get(i);
            testChannel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(chunk, 0, chunk.length)));
        }
        testChannel.writeInbound(EMPTY_LAST_CONTENT);

        assertEquals(5, testChannel.inboundMessages().size());

        testChannel.readInbound(); // Discard beginning HttpResponse.

        final ByteBuf firstContent = testChannel.<HttpContent>readInbound().content();
        assertTrue("aaa".contentEquals(firstContent.readCharSequence(firstContent.readableBytes(), CharsetUtil.UTF_8)));

        final ByteBuf secondContent = testChannel.<HttpContent>readInbound().content();
        assertTrue("bbb".contentEquals(secondContent.readCharSequence(secondContent.readableBytes(), CharsetUtil.UTF_8)));

        final ByteBuf thirdContent = testChannel.<HttpContent>readInbound().content();
        assertTrue("ccc".contentEquals(thirdContent.readCharSequence(thirdContent.readableBytes(), CharsetUtil.UTF_8)));
    }

    @Test
    public void shouldHandleEmptyFrameDecompression() throws Exception {
        final Deflater compressor = new Deflater();
        compressor.setInput(new byte[0]);
        compressor.finish();
        final byte[] compressedArray = new byte[8]; // length of when zlib encodes empty buffer.
        compressor.deflate(compressedArray);

        final HttpContentDecompressionHandler decompressionHandler = new HttpContentDecompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(decompressionHandler);
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final FullHttpResponse in = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(compressedArray), headers, new DefaultHttpHeaders());

        testChannel.writeInbound(in);
        final FullHttpResponse result = testChannel.readInbound();
        assertEquals(0, result.content().readableBytes());
    }

    @Test
    public void shouldFireExceptionWhenContentIncorrectlyCompressed() {
        final HttpContentDecompressionHandler decompressionHandler = new HttpContentDecompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(decompressionHandler);
        final ByteBuf content = testChannel.alloc().buffer();
        content.writeCharSequence("abc", CharsetUtil.UTF_8);
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, content, headers, headers);

        try {
            testChannel.writeInbound(response);
            fail("Expected exception.");
        } catch (Exception e) {
            assertTrue(e instanceof DataFormatException);
        }
    }

    @Test
    public void shouldHandleUncompressedBinary() throws Exception {
        final int arraySize = 128;
        final byte[] largeArray = new byte[arraySize];
        new Random().nextBytes(largeArray);

        final Deflater compressor = new Deflater();
        compressor.setInput(largeArray);
        compressor.finish();
        final byte[] compressedArray = new byte[arraySize + 50]; // Added buffer since binary probably won't compress.
        final int len = compressor.deflate(compressedArray, 0, compressedArray.length, Deflater.FULL_FLUSH);

        final HttpContentDecompressionHandler decompressionHandler = new HttpContentDecompressionHandler();
        final EmbeddedChannel testChannel = new EmbeddedChannel(decompressionHandler);
        final HttpHeaders headers = new DefaultHttpHeaders().add(CONTENT_ENCODING, DEFLATE);
        final FullHttpResponse inbound = new DefaultFullHttpResponse(
                HTTP_1_1, OK, Unpooled.wrappedBuffer(compressedArray, 0, len), headers, new DefaultHttpHeaders());

        testChannel.writeInbound(inbound);

        final FullHttpResponse response = testChannel.readInbound();
        final byte[] contentBytes = new byte[response.content().readableBytes()];
        response.content().readBytes(contentBytes);
        response.release();

        assertArrayEquals(largeArray, contentBytes);
    }
}
