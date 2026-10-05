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
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.stream.ByteBufQueueInputStream;
import org.apache.tinkerpop.gremlin.driver.stream.GraphBinaryStreamResponseReader;
import org.apache.tinkerpop.gremlin.driver.stream.InputStreamBuffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

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

    private ResultSet runAgainstRawPayload(final ByteBuf payload) {
        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);

        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(payload);
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        new GraphBinaryStreamResponseReader(buffer, reader, rs, pending).run();
        return rs;
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
        final String value = String.join("", java.util.Collections.nCopies(200, "0123456789"));
        final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        final ByteBuf fullPayload = Unpooled.buffer();
        fullPayload.writeByte(GraphBinaryWriterVersionByte());
        fullPayload.writeByte(0);
        fullPayload.writeByte(DataType.STRING.getCodeByte());
        fullPayload.writeByte(0);
        fullPayload.writeInt(valueBytes.length);
        fullPayload.writeBytes(valueBytes);
        fullPayload.writeByte(DataType.MARKER.getCodeByte());
        fullPayload.writeByte(0);
        fullPayload.writeByte(0);

        fullPayload.writeInt(0);
        fullPayload.writeByte(1);
        fullPayload.writeByte(1);

        final ResultSet rs = new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);

        final int splitPoint = fullPayload.readableBytes() / 2;
        final ByteBuf chunk1 = fullPayload.readSlice(splitPoint).retain();
        final ByteBuf chunk2 = fullPayload.retain();

        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(chunk1);
        stream.offer(chunk2);
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        new GraphBinaryStreamResponseReader(buffer, reader, rs, pending).run();

        assertTrue(rs.allItemsAvailable());
        final java.util.List<org.apache.tinkerpop.gremlin.driver.Result> results = rs.all().get();
        org.junit.Assert.assertEquals(1, results.size());
        org.junit.Assert.assertEquals(value, results.get(0).getString());

        fullPayload.release();
    }

    private static byte GraphBinaryWriterVersionByte() {
        return (byte) 0x84;
    }
}
