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
package org.apache.tinkerpop.gremlin.util.ser.binary;

import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A GraphBinary value that carries a length or element-count prefix must not size an allocation from that prefix
 * without validating it. A crafted prefix (negative, or larger than the bytes actually present) is otherwise a
 * pre-auth memory-amplification denial of service: a tiny message drives a multi-gigabyte allocation. These tests
 * feed such prefixes with no following elements and assert the reader refuses them rather than attempting the
 * allocation.
 */
public class GraphBinaryLengthPrefixTest {

    private final GraphBinaryReader reader = new GraphBinaryReader();
    private final GraphBinaryWriter writer = new GraphBinaryWriter();
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private void assertRejectsLengthPrefix(final DataType type, final int declaredLength) {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(type.getCodeByte());  // {type_code}
            buffer.writeByte(0);                    // {value_flag} = non-null
            buffer.writeInt(declaredLength);        // {length} with no elements following
            reader.read(buffer);
            fail(String.format("read of %s with declared length %d must be refused", type, declaredLength));
        } catch (IOException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("length prefix"));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectOversizedListLengthPrefix() {
        assertRejectsLengthPrefix(DataType.LIST, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectOversizedSetLengthPrefix() {
        assertRejectsLengthPrefix(DataType.SET, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectOversizedMapLengthPrefix() {
        assertRejectsLengthPrefix(DataType.MAP, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectOversizedStringLengthPrefix() {
        assertRejectsLengthPrefix(DataType.STRING, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectOversizedByteBufferLengthPrefix() {
        assertRejectsLengthPrefix(DataType.BYTEBUFFER, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectOversizedTreeLengthPrefix() {
        assertRejectsLengthPrefix(DataType.TREE, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectNegativeListLengthPrefix() {
        assertRejectsLengthPrefix(DataType.LIST, -1);
    }

    @Test
    public void shouldRejectNegativeByteBufferLengthPrefix() {
        assertRejectsLengthPrefix(DataType.BYTEBUFFER, -1);
    }

    @Test
    public void shouldRejectNegativeTreeLengthPrefix() {
        assertRejectsLengthPrefix(DataType.TREE, -1);
    }

    @Test
    public void shouldRejectTruncatedListLengthPrefix() {
        assertRejectsTruncatedLengthPrefix(DataType.LIST);
    }

    @Test
    public void shouldRejectTruncatedStringLengthPrefix() {
        assertRejectsTruncatedLengthPrefix(DataType.STRING);
    }

    @Test
    public void shouldRejectNegativeBulkSetBulk() {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.BULKSET.getCodeByte());  // {type_code}
            buffer.writeByte(0);                                // {value_flag} = non-null
            buffer.writeInt(1);                                 // element count = 1
            buffer.writeByte(DataType.INT.getCodeByte());       // one fully-qualified INT element
            buffer.writeByte(0);
            buffer.writeInt(42);
            buffer.writeLong(-1L);                              // negative per-element bulk
            reader.read(buffer);
            fail("read of a BulkSet with a negative bulk must be refused");
        } catch (IOException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("bulk"));
        } finally {
            buffer.release();
        }
    }

    private void assertRejectsTruncatedLengthPrefix(final DataType type) {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(type.getCodeByte());   // {type_code}
            buffer.writeByte(0);                     // {value_flag} = non-null
            buffer.writeByte(0);                     // only 2 of the 4 length-prefix bytes present
            buffer.writeByte(0);
            reader.read(buffer);
            fail(String.format("read of %s with a truncated length prefix must be refused", type));
        } catch (IOException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("length prefix"));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRoundTripListLargerThanPreallocCap() throws IOException {
        // more elements than the internal pre-allocation cap, to prove the cap bounds only the initial capacity
        // and the container still grows to hold every element (the cap must never be used as the read-loop bound)
        assertRoundTripsFully(buildIntList(20000));
    }

    @Test
    public void shouldRoundTripMapLargerThanPreallocCap() throws IOException {
        final Map<Integer, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < 20000; i++) map.put(i, i);
        assertRoundTripsFully(map);
    }

    private List<Integer> buildIntList(final int count) {
        final List<Integer> list = new ArrayList<>();
        for (int i = 0; i < count; i++) list.add(i);
        return list;
    }

    private void assertRoundTripsFully(final Object value) throws IOException {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            writer.write(value, buffer);
            assertEquals(value, reader.read(buffer));
        } finally {
            buffer.release();
        }
    }
}
