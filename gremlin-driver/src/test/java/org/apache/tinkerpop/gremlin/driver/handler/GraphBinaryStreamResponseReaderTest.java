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
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.stream.ByteBufQueueInputStream;
import org.apache.tinkerpop.gremlin.driver.stream.GraphBinaryStreamResponseReader;
import org.apache.tinkerpop.gremlin.driver.stream.InputStreamBuffer;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GraphBinaryStreamResponseReaderTest {

    private ExecutorService executor;
    private GraphBinaryMessageSerializerV4 serializer;
    private GraphBinaryReader reader;

    @Before
    public void setup() {
        executor = Executors.newCachedThreadPool();
        serializer = new GraphBinaryMessageSerializerV4();
        reader = serializer.getMapper().getReader();
    }

    @After
    public void teardown() {
        executor.shutdownNow();
    }

    @Test
    public void shouldReadSingleItemResponse() throws Exception {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);

        final ByteBuf payload = serializer.serializeResponseAsBinary(
                ResponseMessage.build().code(HttpResponseStatus.OK)
                        .result(Collections.singletonList("hello")).create(),
                ByteBufAllocator.DEFAULT);

        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(payload);
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        new GraphBinaryStreamResponseReader(buffer, reader, rs, pending).run();

        final List<Result> results = rs.all().get();
        assertEquals(1, results.size());
        assertEquals("hello", results.get(0).getString());
        assertNull(pending.get());
    }

    @Test
    public void shouldReadMultipleItemResponse() throws Exception {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);

        final ByteBuf payload = serializer.serializeResponseAsBinary(
                ResponseMessage.build().code(HttpResponseStatus.OK)
                        .result(Arrays.asList(1, 2, 3)).create(),
                ByteBufAllocator.DEFAULT);

        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(payload);
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        new GraphBinaryStreamResponseReader(buffer, reader, rs, pending).run();

        final List<Result> results = rs.all().get();
        assertEquals(3, results.size());
        assertEquals(1, results.get(0).getInt());
        assertEquals(2, results.get(1).getInt());
        assertEquals(3, results.get(2).getInt());
    }

    @Test
    public void shouldReadBulkedResponse() throws Exception {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);

        final ByteBuf payload = serializer.serializeResponseAsBinary(
                ResponseMessage.build().code(HttpResponseStatus.OK)
                        .result(Arrays.asList("a", 3L)).bulked(true).create(),
                ByteBufAllocator.DEFAULT);

        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(payload);
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        new GraphBinaryStreamResponseReader(buffer, reader, rs, pending).run();

        final List<Result> results = rs.all().get();
        assertEquals(1, results.size());
        assertTrue(results.get(0).getObject() instanceof DefaultRemoteTraverser);
        final DefaultRemoteTraverser<?> traverser = (DefaultRemoteTraverser<?>) results.get(0).getObject();
        assertEquals("a", traverser.get());
        assertEquals(3L, traverser.bulk());
    }

    @Test
    public void shouldHandleErrorFooter() throws Exception {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);

        final ByteBuf payload = serializer.serializeResponseAsBinary(
                ResponseMessage.build().code(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                        .statusMessage("Something went wrong")
                        .exception("java.lang.RuntimeException")
                        .result(Collections.emptyList()).create(),
                ByteBufAllocator.DEFAULT);

        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(payload);
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        new GraphBinaryStreamResponseReader(buffer, reader, rs, pending).run();

        assertTrue(rs.allItemsAvailable());
        try {
            rs.all().get();
            fail("Expected exception");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof ResponseException);
            final ResponseException re = (ResponseException) e.getCause();
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
            assertEquals("Something went wrong", re.getMessage());
        }
        assertNull(pending.get());
    }

    @Test
    public void shouldReadDataSplitAcrossChunks() throws Exception {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);

        // Use a large string (~1000 chars) so the split point must land inside it regardless of
        // header/footer overhead. The header+footer are ~30 bytes; the string item is ~1000 bytes.
        final String largeValue = String.join("", Collections.nCopies(100, "abcdefghij"));
        final ByteBuf fullPayload = serializer.serializeResponseAsBinary(
                ResponseMessage.build().code(HttpResponseStatus.OK)
                        .result(Collections.singletonList(largeValue)).create(),
                ByteBufAllocator.DEFAULT);

        final int splitPoint = fullPayload.readableBytes() / 2;
        final ByteBuf chunk1 = fullPayload.readSlice(splitPoint).retain();
        final ByteBuf chunk2 = fullPayload.retain();

        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(chunk1);
        stream.offer(chunk2);
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        new GraphBinaryStreamResponseReader(buffer, reader, rs, pending).run();

        final List<Result> results = rs.all().get();
        assertEquals(1, results.size());
        assertEquals(largeValue, results.get(0).getString());

        fullPayload.release();
    }

    @Test
    public void shouldReadEmptyResponse() throws Exception {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);

        final ByteBuf payload = serializer.serializeResponseAsBinary(
                ResponseMessage.build().code(HttpResponseStatus.OK)
                        .result(Collections.emptyList()).create(),
                ByteBufAllocator.DEFAULT);

        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(payload);
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        new GraphBinaryStreamResponseReader(buffer, reader, rs, pending).run();

        final List<Result> results = rs.all().get();
        assertTrue(results.isEmpty());
        assertNull(pending.get());
    }
}
