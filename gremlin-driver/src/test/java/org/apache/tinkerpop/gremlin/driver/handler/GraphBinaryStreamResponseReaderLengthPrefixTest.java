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
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.stream.ByteBufQueueInputStream;
import org.apache.tinkerpop.gremlin.driver.stream.GraphBinaryStreamResponseReader;
import org.apache.tinkerpop.gremlin.driver.stream.InputStreamBuffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReadLimits;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Confirms that a malformed GraphBinary length/count prefix is rejected on the driver's
 * {@code InputStreamBuffer}-backed streaming response path, where {@link InputStreamBuffer#readableBytes()}
 * throws {@code UnsupportedOperationException} and so cannot be used for validation.
 */
public class GraphBinaryStreamResponseReaderLengthPrefixTest {

    private ExecutorService executor;
    private GraphBinaryReader reader;

    @Before
    public void setup() {
        executor = Executors.newCachedThreadPool();
        reader = new GraphBinaryMessageSerializerV4().getMapper().getReader();
    }

    @After
    public void teardown() {
        executor.shutdownNow();
    }

    private ResultSet runAgainstPayloadChunks(final GraphBinaryReader graphBinaryReader, final ByteBuf... chunks) {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);

        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        for (final ByteBuf chunk : chunks) stream.offer(chunk);
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        new GraphBinaryStreamResponseReader(buffer, graphBinaryReader, rs, pending).run();
        return rs;
    }

    private ResultSet runAgainstRawPayload(final ByteBuf payload) {
        return runAgainstPayloadChunks(reader, payload);
    }

    private static ByteBuf stringResponse(final String value) {
        final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        final ByteBuf payload = Unpooled.buffer();
        payload.writeByte(GraphBinaryWriterVersionByte());
        payload.writeByte(0);
        payload.writeByte(DataType.STRING.getCodeByte());
        payload.writeByte(0);
        payload.writeInt(valueBytes.length);
        payload.writeBytes(valueBytes);
        payload.writeByte(DataType.MARKER.getCodeByte());
        payload.writeByte(0);
        payload.writeByte(0);

        payload.writeInt(0);
        payload.writeByte(1);
        payload.writeByte(1);
        return payload;
    }

    @Test
    public void shouldRejectOversizedStringLengthPrefixOnStreamingBuffer() throws Exception {
        final ByteBuf payload = Unpooled.buffer();
        payload.writeByte(GraphBinaryWriterVersionByte());
        payload.writeByte(0);
        payload.writeByte(DataType.STRING.getCodeByte());
        payload.writeByte(0);
        payload.writeInt(Integer.MAX_VALUE);

        final ResultSet rs = runAgainstRawPayload(payload);
        assertTrue(rs.allItemsAvailable());
        try {
            rs.all().get();
            fail("An oversized String length prefix on the streaming buffer must be refused, not allocated");
        } catch (Exception e) {
        }
    }

    @Test
    public void shouldRejectOversizedListElementCountOnStreamingBuffer() throws Exception {
        final ByteBuf payload = Unpooled.buffer();
        payload.writeByte(GraphBinaryWriterVersionByte());
        payload.writeByte(0);
        payload.writeByte(DataType.LIST.getCodeByte());
        payload.writeByte(0);
        payload.writeInt(Integer.MAX_VALUE);

        final ResultSet rs = runAgainstRawPayload(payload);
        assertTrue(rs.allItemsAvailable());
        try {
            rs.all().get();
            fail("An oversized List element count on the streaming buffer must be refused");
        } catch (Exception e) {
        }
    }

    @Test
    public void shouldStillReadLegitimateStringAcrossChunksOnStreamingBuffer() throws Exception {
        final String value = String.join("", Collections.nCopies(200, "0123456789"));
        final ByteBuf fullPayload = stringResponse(value);

        final int splitPoint = fullPayload.readableBytes() / 2;
        final ByteBuf chunk1 = fullPayload.readSlice(splitPoint).retain();
        final ByteBuf chunk2 = fullPayload.retain();

        final ResultSet rs = runAgainstPayloadChunks(reader, chunk1, chunk2);

        assertTrue(rs.allItemsAvailable());
        final List<Result> results = rs.all().get();
        assertEquals(1, results.size());
        assertEquals(value, results.get(0).getString());

        fullPayload.release();
    }

    @Test
    public void shouldRejectValueAboveConfiguredLimitOnStreamingBuffer() throws Exception {
        final GraphBinaryReader limited = reader.withLimits(GraphBinaryReadLimits.of(100, 100, 100));
        final String value = String.join("", Collections.nCopies(200, "x"));

        final ResultSet rs = runAgainstPayloadChunks(limited, stringResponse(value));

        assertTrue(rs.allItemsAvailable());
        try {
            rs.all().get();
            fail("A value above the configured limit on the streaming buffer must be refused");
        } catch (Exception e) {
        }
    }

    @Test
    public void shouldReadValueAboveDefaultLimitOnStreamingBufferWhenLimitIsRaised() throws Exception {
        final String value = String.join("", Collections.nCopies(200, "x"));
        final GraphBinaryReader lowered = reader.withLimits(GraphBinaryReadLimits.of(100, 100, 100));
        final GraphBinaryReader raised = lowered.withLimits(GraphBinaryReadLimits.of(1000, 100, 100));

        final ResultSet rejected = runAgainstPayloadChunks(lowered, stringResponse(value));
        assertTrue(rejected.allItemsAvailable());
        try {
            rejected.all().get();
            fail("The lowered limit should have refused the value");
        } catch (Exception e) {
        }

        final ResultSet accepted = runAgainstPayloadChunks(raised, stringResponse(value));
        assertTrue(accepted.allItemsAvailable());
        assertEquals(value, accepted.all().get().get(0).getString());
    }

    private static byte GraphBinaryWriterVersionByte() {
        return (byte) 0x84;
    }
}
