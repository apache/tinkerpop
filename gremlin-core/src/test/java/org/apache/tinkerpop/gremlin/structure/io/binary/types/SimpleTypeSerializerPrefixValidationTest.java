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
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReadLimits;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SimpleTypeSerializerPrefixValidationTest {

    private static final GraphBinaryReader defaultReader = new GraphBinaryReader();

    private static GraphBinaryReader readerWith(final int maxValueLength, final int maxArrayLength, final int maxInitialCapacity) {
        return new GraphBinaryReader(TypeSerializerRegistry.INSTANCE, null,
                GraphBinaryReadLimits.of(maxValueLength, maxArrayLength, maxInitialCapacity));
    }

    @Test
    public void readByteLengthShouldRejectNegativeOnKnownLengthBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readByteLength(knownLengthBuffer(-1, 100), defaultReader));
    }

    @Test
    public void readByteLengthShouldRejectNegativeOnStreamingBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readByteLength(streamingBuffer(-1), defaultReader));
    }

    @Test
    public void readByteLengthShouldRejectLengthExceedingReadableBytesOnKnownLengthBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readByteLength(knownLengthBuffer(1000, 10), defaultReader));
    }

    @Test
    public void readByteLengthShouldRejectImplausibleLengthOnStreamingBufferEvenWithoutReadableBytes() {
        assertRejects(() -> SimpleTypeSerializer.readByteLength(streamingBuffer(Integer.MAX_VALUE), defaultReader));
    }

    @Test
    public void readByteLengthShouldAcceptValidLengthOnKnownLengthBuffer() throws IOException {
        assertEquals(10, SimpleTypeSerializer.readByteLength(knownLengthBuffer(10, 100), defaultReader));
    }

    @Test
    public void readByteLengthShouldAcceptValidLengthOnStreamingBuffer() throws IOException {
        assertEquals(10, SimpleTypeSerializer.readByteLength(streamingBuffer(10), defaultReader));
    }

    @Test
    public void readByteLengthShouldRejectLengthAboveConfiguredLimit() throws IOException {
        final GraphBinaryReader reader = readerWith(100, 100, 100);
        assertRejects(() -> SimpleTypeSerializer.readByteLength(streamingBuffer(101), reader));
        assertEquals(100, SimpleTypeSerializer.readByteLength(streamingBuffer(100), reader));
    }

    @Test
    public void readByteLengthShouldAcceptLengthAboveDefaultLimitWhenRaised() throws IOException {
        final int aboveDefault = GraphBinaryReadLimits.DEFAULT_MAX_VALUE_LENGTH + 1;
        assertRejects(() -> SimpleTypeSerializer.readByteLength(streamingBuffer(aboveDefault), defaultReader));
        assertRejects(() -> SimpleTypeSerializer.readByteLength(knownLengthBuffer(aboveDefault, Integer.MAX_VALUE), defaultReader));

        final GraphBinaryReader raised = readerWith(Integer.MAX_VALUE, 100, 100);
        assertEquals(aboveDefault, SimpleTypeSerializer.readByteLength(streamingBuffer(aboveDefault), raised));
        assertEquals(aboveDefault, SimpleTypeSerializer.readByteLength(knownLengthBuffer(aboveDefault, Integer.MAX_VALUE), raised));
    }

    @Test
    public void readByteLengthShouldStillRejectLengthExceedingReadableBytesWhenLimitRaised() {
        final GraphBinaryReader raised = readerWith(Integer.MAX_VALUE, 100, 100);
        assertRejects(() -> SimpleTypeSerializer.readByteLength(knownLengthBuffer(1000, 10), raised));
    }

    @Test
    public void readElementCountShouldRejectNegativeOnKnownLengthBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readElementCount(knownLengthBuffer(-1, 100)));
    }

    @Test
    public void readElementCountShouldRejectNegativeOnStreamingBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readElementCount(streamingBuffer(-1)));
    }

    @Test
    public void readElementCountShouldRejectCountExceedingReadableBytesOnKnownLengthBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readElementCount(knownLengthBuffer(1000, 10)));
    }

    @Test
    public void readElementCountShouldAcceptValidCountOnKnownLengthBuffer() throws IOException {
        assertEquals(5, SimpleTypeSerializer.readElementCount(knownLengthBuffer(5, 100)));
    }

    @Test
    public void readElementCountShouldAcceptLargeCountOnStreamingBufferWithoutComparingToReadableBytes() throws IOException {
        assertEquals(1_000_000, SimpleTypeSerializer.readElementCount(streamingBuffer(1_000_000)));
    }

    @Test
    public void readFixedArrayCountShouldRejectNegativeOnKnownLengthBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readFixedArrayCount(knownLengthBuffer(-1, 100), defaultReader));
    }

    @Test
    public void readFixedArrayCountShouldRejectNegativeOnStreamingBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readFixedArrayCount(streamingBuffer(-1), defaultReader));
    }

    @Test
    public void readFixedArrayCountShouldRejectCountExceedingReadableBytesOnKnownLengthBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readFixedArrayCount(knownLengthBuffer(1000, 10), defaultReader));
    }

    @Test
    public void readFixedArrayCountShouldRejectImplausibleCountOnStreamingBufferEvenWithoutReadableBytes() {
        assertRejects(() -> SimpleTypeSerializer.readFixedArrayCount(streamingBuffer(2_000_000), defaultReader));
    }

    @Test
    public void readFixedArrayCountShouldAcceptValidCountOnStreamingBuffer() throws IOException {
        assertEquals(10, SimpleTypeSerializer.readFixedArrayCount(streamingBuffer(10), defaultReader));
    }

    @Test
    public void readFixedArrayCountShouldHonorConfiguredLimit() throws IOException {
        final GraphBinaryReader lowered = readerWith(100, 5, 100);
        assertRejects(() -> SimpleTypeSerializer.readFixedArrayCount(streamingBuffer(6), lowered));
        assertEquals(5, SimpleTypeSerializer.readFixedArrayCount(streamingBuffer(5), lowered));

        final GraphBinaryReader raised = readerWith(100, 3_000_000, 100);
        assertEquals(2_000_000, SimpleTypeSerializer.readFixedArrayCount(streamingBuffer(2_000_000), raised));
    }

    @Test
    public void cappedInitialCapacityShouldPassThroughSmallCount() {
        assertEquals(10, SimpleTypeSerializer.cappedInitialCapacity(10, defaultReader));
    }

    @Test
    public void cappedInitialCapacityShouldHonorConfiguredLimit() {
        assertEquals(50, SimpleTypeSerializer.cappedInitialCapacity(10_000_000, readerWith(100, 100, 50)));
        assertEquals(10_000_000, SimpleTypeSerializer.cappedInitialCapacity(10_000_000, readerWith(100, 100, 20_000_000)));
    }

    @Test
    public void cappedInitialCapacityShouldCapLargeCount() {
        final int capped = SimpleTypeSerializer.cappedInitialCapacity(10_000_000, defaultReader);
        if (capped >= 10_000_000) {
            fail("A large declared count must not be used directly as a pre-allocated capacity, was: " + capped);
        }
    }

    private interface ThrowingRunnable {
        void run() throws IOException;
    }

    private static void assertRejects(final ThrowingRunnable r) {
        try {
            r.run();
            fail("Expected an IOException to be thrown for an invalid length/count prefix");
        } catch (IOException expected) {
        }
    }

    private static Buffer knownLengthBuffer(final int valueToRead, final int readableBytes) {
        return new PrefixOnlyBuffer(valueToRead, readableBytes, true);
    }

    private static Buffer streamingBuffer(final int valueToRead) {
        return new PrefixOnlyBuffer(valueToRead, -1, false);
    }

    private static final class PrefixOnlyBuffer implements Buffer {
        private final int valueToRead;
        private final int readableBytes;
        private final boolean hasKnownRemainingLength;

        PrefixOnlyBuffer(final int valueToRead, final int readableBytes, final boolean hasKnownRemainingLength) {
            this.valueToRead = valueToRead;
            this.readableBytes = readableBytes;
            this.hasKnownRemainingLength = hasKnownRemainingLength;
        }

        @Override
        public int readInt() {
            return valueToRead;
        }

        @Override
        public int readableBytes() {
            if (!hasKnownRemainingLength) {
                throw new UnsupportedOperationException("readableBytes() is not supported on a streaming Buffer");
            }
            return readableBytes;
        }

        @Override
        public boolean hasKnownRemainingLength() {
            return hasKnownRemainingLength;
        }

        @Override
        public int readerIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer readerIndex(final int readerIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int writerIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writerIndex(final int writerIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer markWriterIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer resetWriterIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int capacity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDirect() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean readBoolean() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte readByte() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short readShort() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long readLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float readFloat() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double readDouble() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer readBytes(final byte[] destination) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer readBytes(final byte[] destination, final int dstIndex, final int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer readBytes(final ByteBuffer dst) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer readBytes(final OutputStream out, final int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writeBoolean(final boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writeByte(final int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writeShort(final int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writeInt(final int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writeLong(final long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writeFloat(final float value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writeDouble(final double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writeBytes(final byte[] src) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writeBytes(final ByteBuffer src) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer writeBytes(final byte[] src, final int srcIndex, final int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean release() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer retain() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int referenceCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nioBufferCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer[] nioBuffers() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer[] nioBuffers(final int index, final int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer nioBuffer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer nioBuffer(final int index, final int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Buffer getBytes(final int index, final byte[] dst) {
            throw new UnsupportedOperationException();
        }
    }
}
