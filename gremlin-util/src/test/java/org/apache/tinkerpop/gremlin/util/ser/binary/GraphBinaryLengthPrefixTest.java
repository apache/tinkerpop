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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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

    // distinct sentinels below Integer.MAX_VALUE, so each Bytecode prefix is identified by its own value in the
    // rejection message. These counts drive a read loop rather than sizing an array, so they need no headroom.
    private static final int OVERSIZED_STEPS_LENGTH = 1_000_000_007;
    private static final int OVERSIZED_SOURCES_LENGTH = 2_000_000_011;

    // distinct sentinels for the three Graph counts, each far beyond any readable byte count and far above the small
    // legitimate counts that precede them in these frames, so the value reported back identifies which count failed
    private static final int OVERSIZED_GRAPH_VERTEX_COUNT = 1_100_000_009;
    private static final int OVERSIZED_GRAPH_EDGE_COUNT = 1_200_000_017;
    private static final int OVERSIZED_GRAPH_VERTEX_PROPERTY_COUNT = 1_300_000_027;

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
    public void shouldRejectOversizedPArgumentCount() {
        // the argument count immediately sizes fixed Object[]/Class[] arrays, so Integer.MAX_VALUE is a safe
        // sentinel: without the guard the JVM array-size limit fails fast instead of attempting a legal allocation
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.P.getCodeByte());  // {type_code}
            buffer.writeByte(0);                         // {value_flag} = non-null
            writeNonNullableString(buffer, "eq");        // a valid predicate name precedes the count
            buffer.writeInt(Integer.MAX_VALUE);          // {length} with no arguments following
            assertRejectsDeclaredCount(buffer, "P argument count", Integer.MAX_VALUE);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectOversizedBytecodeStepsLength() {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.BYTECODE.getCodeByte());  // {type_code}
            buffer.writeByte(0);                                 // {value_flag} = non-null
            buffer.writeInt(OVERSIZED_STEPS_LENGTH);             // {steps_length} with no steps following
            assertRejectsDeclaredCount(buffer, "Bytecode steps length", OVERSIZED_STEPS_LENGTH);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectOversizedBytecodeSourcesLength() {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.BYTECODE.getCodeByte());  // {type_code}
            buffer.writeByte(0);                                 // {value_flag} = non-null
            buffer.writeInt(0);                                  // a valid, empty {steps_length}
            buffer.writeInt(OVERSIZED_SOURCES_LENGTH);           // {sources_length} with no sources following
            assertRejectsDeclaredCount(buffer, "Bytecode sources length", OVERSIZED_SOURCES_LENGTH);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectOversizedBytecodeInstructionValuesLength() {
        // the instruction argument count immediately sizes a fixed Object[], so Integer.MAX_VALUE fails fast at the
        // JVM array-size limit if the guard is absent
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.BYTECODE.getCodeByte());  // {type_code}
            buffer.writeByte(0);                                 // {value_flag} = non-null
            buffer.writeInt(1);                                  // one step follows
            writeNonNullableString(buffer, "V");                 // a valid step operator
            buffer.writeInt(Integer.MAX_VALUE);                  // {values_length} with no arguments following
            assertRejectsDeclaredCount(buffer, "Bytecode instruction values length", Integer.MAX_VALUE);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectOversizedGraphVertexCountPrefix() {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.GRAPH.getCodeByte());     // {type_code}
            buffer.writeByte(0);                                 // {value_flag} = non-null
            buffer.writeInt(OVERSIZED_GRAPH_VERTEX_COUNT);       // {vertex_count} with no vertices following
            assertRejectsDeclaredCount(buffer, "Graph vertex count", OVERSIZED_GRAPH_VERTEX_COUNT);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectOversizedGraphEdgeCountPrefix() {
        // zero vertices is a legitimate count, so the vertex loop is skipped and the edge count is what gets refused
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.GRAPH.getCodeByte());     // {type_code}
            buffer.writeByte(0);                                 // {value_flag} = non-null
            buffer.writeInt(0);                                  // a valid, empty {vertex_count}
            buffer.writeInt(OVERSIZED_GRAPH_EDGE_COUNT);         // {edge_count} with no edges following
            assertRejectsDeclaredCount(buffer, "Graph edge count", OVERSIZED_GRAPH_EDGE_COUNT);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectOversizedGraphVertexPropertyCountPrefix() {
        // one complete, valid vertex is read first, so the reader is well past the outer count when the per-vertex
        // vertex-property count is refused
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.GRAPH.getCodeByte());     // {type_code}
            buffer.writeByte(0);                                 // {value_flag} = non-null
            buffer.writeInt(1);                                  // one vertex follows
            buffer.writeByte(DataType.INT.getCodeByte());        // the vertex id, fully qualified
            buffer.writeByte(0);
            buffer.writeInt(1);
            writeNonNullableString(buffer, "v");                 // the vertex label
            buffer.writeInt(OVERSIZED_GRAPH_VERTEX_PROPERTY_COUNT);  // {vp_count} with no properties following
            assertRejectsDeclaredCount(buffer, "Graph vertex property count",
                    OVERSIZED_GRAPH_VERTEX_PROPERTY_COUNT);
        } finally {
            buffer.release();
        }
    }

    /**
     * Reads a crafted message that is expected to be refused because of {@code declaredCount}, and requires the
     * failure to name that specific prefix. Matching on the declared count as well as the "length prefix" wording
     * keeps these tests from passing on an unrelated downstream failure: a message truncated after the count is also
     * refused for lacking a length prefix, but only the validated count is reported back with its own value.
     */
    private void assertRejectsDeclaredCount(final Buffer buffer, final String prefixName, final int declaredCount) {
        try {
            final Object result = reader.read(buffer);
            fail(String.format("read of a %s of %d must be refused, but returned %s", prefixName, declaredCount,
                    result));
        } catch (IOException expected) {
            assertThat(String.format("%s of %d was refused, but not by the length prefix validation: %s", prefixName,
                    declaredCount, describeCausalChain(expected)),
                    reportsLengthPrefix(expected, declaredCount), is(true));
        }
    }

    /**
     * Looks for a length-prefix rejection naming {@code declaredCount} anywhere in the causal chain, since a nested
     * read failure may be wrapped by the serializer that requested it.
     */
    private static boolean reportsLengthPrefix(final Throwable thrown, final int declaredCount) {
        final String declared = Integer.toString(declaredCount);
        for (Throwable current = thrown; current != null; current = current.getCause()) {
            final String message = current.getMessage();
            if (message != null && message.contains("length prefix") && message.contains(declared))
                return true;
        }
        return false;
    }

    private static String describeCausalChain(final Throwable thrown) {
        final StringBuilder sb = new StringBuilder();
        for (Throwable current = thrown; current != null; current = current.getCause()) {
            if (sb.length() > 0) sb.append(" caused by ");
            sb.append(current);
        }
        return sb.toString();
    }

    /**
     * Writes a {@code String} in the non-nullable form the GraphBinary string serializer expects, which is the UTF-8
     * byte length as an int followed by those bytes, with no type code or value flag.
     */
    private static void writeNonNullableString(final Buffer buffer, final String value) {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
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
