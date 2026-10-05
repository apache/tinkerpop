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
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Exercises the prefix validation helpers in {@link SimpleTypeSerializer} against a buffer that knows its
 * remaining length ({@link Buffer#hasKnownRemainingLength()} is {@code true}) and one that does not
 * ({@code false}, matching a streaming HTTP response buffer).
 */
public class SimpleTypeSerializerPrefixValidationTest {

    @Test
    public void readByteLengthShouldRejectNegativeOnKnownLengthBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readByteLength(knownLengthBuffer(-1, 100)));
    }

    @Test
    public void readByteLengthShouldRejectNegativeOnStreamingBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readByteLength(streamingBuffer(-1)));
    }

    @Test
    public void readByteLengthShouldRejectLengthExceedingReadableBytesOnKnownLengthBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readByteLength(knownLengthBuffer(1000, 10)));
    }

    @Test
    public void readByteLengthShouldRejectImplausibleLengthOnStreamingBufferEvenWithoutReadableBytes() {
        assertRejects(() -> SimpleTypeSerializer.readByteLength(streamingBuffer(Integer.MAX_VALUE)));
    }

    @Test
    public void readByteLengthShouldAcceptValidLengthOnKnownLengthBuffer() throws IOException {
        assertEquals(10, SimpleTypeSerializer.readByteLength(knownLengthBuffer(10, 100)));
    }

    @Test
    public void readByteLengthShouldAcceptValidLengthOnStreamingBuffer() throws IOException {
        assertEquals(10, SimpleTypeSerializer.readByteLength(streamingBuffer(10)));
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
        assertRejects(() -> SimpleTypeSerializer.readFixedArrayCount(knownLengthBuffer(-1, 100)));
    }

    @Test
    public void readFixedArrayCountShouldRejectNegativeOnStreamingBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readFixedArrayCount(streamingBuffer(-1)));
    }

    @Test
    public void readFixedArrayCountShouldRejectCountExceedingReadableBytesOnKnownLengthBuffer() {
        assertRejects(() -> SimpleTypeSerializer.readFixedArrayCount(knownLengthBuffer(1000, 10)));
    }

    @Test
    public void readFixedArrayCountShouldRejectImplausibleCountOnStreamingBufferEvenWithoutReadableBytes() {
        assertRejects(() -> SimpleTypeSerializer.readFixedArrayCount(streamingBuffer(2_000_000)));
    }

    @Test
    public void readFixedArrayCountShouldAcceptValidCountOnStreamingBuffer() throws IOException {
        assertEquals(10, SimpleTypeSerializer.readFixedArrayCount(streamingBuffer(10)));
    }

    @Test
    public void cappedInitialCapacityShouldPassThroughSmallCount() {
        assertEquals(10, SimpleTypeSerializer.cappedInitialCapacity(10));
    }

    @Test
    public void cappedInitialCapacityShouldCapLargeCount() {
        final int capped = SimpleTypeSerializer.cappedInitialCapacity(10_000_000);
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
