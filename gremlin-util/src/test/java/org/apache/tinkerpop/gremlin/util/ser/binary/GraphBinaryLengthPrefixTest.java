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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Feeds malformed length/count prefixes through {@link GraphBinaryReader} against a fully aggregated buffer and
 * asserts they are refused with an {@link IOException} rather than used to size an allocation.
 * <p>
 * See also {@code GraphBinaryStreamResponseReaderLengthPrefixTest} in {@code gremlin-driver} for the equivalent
 * checks against the driver's streaming response buffer, which cannot report {@code readableBytes()}.
 */
public class GraphBinaryLengthPrefixTest {

    private final GraphBinaryReader reader = new GraphBinaryReader();
    private final GraphBinaryWriter writer = new GraphBinaryWriter();
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private void assertRejectsLengthPrefix(final DataType type, final int declaredLength) {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(type.getCodeByte());
            buffer.writeByte(0);
            buffer.writeInt(declaredLength);
            reader.read(buffer);
            fail(String.format("read of %s with declared length %d must be refused", type, declaredLength));
        } catch (IOException expected) {
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectOversizedStringLengthPrefix() {
        assertRejectsLengthPrefix(DataType.STRING, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectNegativeStringLengthPrefix() {
        assertRejectsLengthPrefix(DataType.STRING, -1);
    }

    @Test
    public void shouldRejectOversizedBigIntegerLengthPrefix() {
        assertRejectsLengthPrefix(DataType.BIGINTEGER, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectNegativeBigIntegerLengthPrefix() {
        assertRejectsLengthPrefix(DataType.BIGINTEGER, -1);
    }

    @Test
    public void shouldRejectOversizedBinaryLengthPrefix() {
        assertRejectsLengthPrefix(DataType.BINARY, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectNegativeBinaryLengthPrefix() {
        assertRejectsLengthPrefix(DataType.BINARY, -1);
    }

    @Test
    public void shouldRejectOversizedListLengthPrefix() {
        assertRejectsLengthPrefix(DataType.LIST, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectNegativeListLengthPrefix() {
        assertRejectsLengthPrefix(DataType.LIST, -1);
    }

    @Test
    public void shouldRejectOversizedSetLengthPrefix() {
        assertRejectsLengthPrefix(DataType.SET, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectNegativeSetLengthPrefix() {
        assertRejectsLengthPrefix(DataType.SET, -1);
    }

    @Test
    public void shouldRejectOversizedMapLengthPrefix() {
        assertRejectsLengthPrefix(DataType.MAP, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectNegativeMapLengthPrefix() {
        assertRejectsLengthPrefix(DataType.MAP, -1);
    }

    @Test
    public void shouldRejectOversizedTreeLengthPrefix() {
        assertRejectsLengthPrefix(DataType.TREE, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectNegativeTreeLengthPrefix() {
        assertRejectsLengthPrefix(DataType.TREE, -1);
    }

    @Test
    public void shouldRejectOversizedGraphVertexCountPrefix() {
        assertRejectsLengthPrefix(DataType.GRAPH, Integer.MAX_VALUE);
    }

    @Test
    public void shouldRejectNegativeGraphVertexCountPrefix() {
        assertRejectsLengthPrefix(DataType.GRAPH, -1);
    }

    @Test
    public void shouldRejectOversizedGraphEdgeCountPrefix() {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.GRAPH.getCodeByte());
            buffer.writeByte(0);
            buffer.writeInt(0);
            buffer.writeInt(Integer.MAX_VALUE);
            reader.read(buffer);
            fail("read of Graph with an oversized edge count must be refused");
        } catch (IOException expected) {
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectOversizedGraphVertexPropertyCountPrefix() {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.GRAPH.getCodeByte());
            buffer.writeByte(0);
            buffer.writeInt(1);
            writer.writeValue(1, buffer, false);
            writer.writeValue(java.util.Collections.singletonList("person"), buffer, false);
            buffer.writeInt(Integer.MAX_VALUE);
            reader.read(buffer);
            fail("read of Graph with an oversized vertex property count must be refused");
        } catch (IOException expected) {
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectNegativeBulkedListBulk() {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.LIST.getCodeByte());
            buffer.writeByte(2);
            buffer.writeInt(1);
            buffer.writeByte(DataType.INT.getCodeByte());
            buffer.writeByte(0);
            buffer.writeInt(42);
            buffer.writeLong(-1L);
            reader.read(buffer);
            fail("read of a bulked List with a negative bulk must be refused");
        } catch (IOException expected) {
            assertThat(expected.getMessage(), containsString("bulk"));
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRejectTruncatedStringLengthPrefix() {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            buffer.writeByte(DataType.STRING.getCodeByte());
            buffer.writeByte(0);
            buffer.writeByte(0);
            buffer.writeByte(0);
            reader.read(buffer);
            fail("read of String with a truncated length prefix must be refused");
        } catch (IOException | IndexOutOfBoundsException expected) {
        } finally {
            buffer.release();
        }
    }

    @Test
    public void shouldRoundTripValidStringAfterGuardIsInPlace() throws IOException {
        final Buffer buffer = bufferFactory.create(ByteBufAllocator.DEFAULT.buffer());
        try {
            writer.write("a perfectly ordinary string", buffer);
            assertEquals("a perfectly ordinary string", reader.read(buffer));
        } finally {
            buffer.release();
        }
    }
}
