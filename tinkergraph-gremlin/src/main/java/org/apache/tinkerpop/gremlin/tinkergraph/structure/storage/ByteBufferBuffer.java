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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A self-contained, heap {@code byte[]}-backed implementation of the gremlin-core {@link Buffer} abstraction, used to
 * drive {@code GraphBinaryWriter}/{@code GraphBinaryReader} without depending on gremlin-util's Netty-backed buffer.
 * All multi-byte values are big-endian, matching the wire order the Netty implementation uses. The backing array grows
 * as needed on write. This class is not thread-safe; a buffer is used by a single thread for a single
 * serialize/deserialize.
 */
public final class ByteBufferBuffer implements Buffer {

    private static final int DEFAULT_CAPACITY = 256;

    private byte[] array;
    private int readerIndex = 0;
    private int writerIndex = 0;
    private int markedWriterIndex = 0;
    private int referenceCount = 1;

    public ByteBufferBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public ByteBufferBuffer(final int initialCapacity) {
        this.array = new byte[Math.max(initialCapacity, 1)];
    }

    /**
     * Wraps an existing array for reading. The writer index is positioned at the end of the supplied data.
     */
    public ByteBufferBuffer(final byte[] data) {
        this.array = data;
        this.writerIndex = data.length;
    }

    /**
     * Returns a copy of the readable-region bytes (from reader index to writer index). Does not change indexes.
     */
    public byte[] toReadableArray() {
        return Arrays.copyOfRange(array, readerIndex, writerIndex);
    }

    /**
     * Returns a copy of all written bytes (index 0 to writer index). Does not change indexes.
     */
    public byte[] toWrittenArray() {
        return Arrays.copyOfRange(array, 0, writerIndex);
    }

    private void ensureWritable(final int additional) {
        final int required = writerIndex + additional;
        if (required <= array.length)
            return;
        int newCapacity = array.length;
        while (newCapacity < required)
            newCapacity <<= 1;
        array = Arrays.copyOf(array, newCapacity);
    }

    private void checkReadable(final int length) {
        if (readerIndex + length > writerIndex)
            throw new IndexOutOfBoundsException(String.format(
                    "Not enough readable bytes: need %d at index %d but writer index is %d", length, readerIndex, writerIndex));
    }

    @Override
    public int readableBytes() {
        return writerIndex - readerIndex;
    }

    @Override
    public int readerIndex() {
        return readerIndex;
    }

    @Override
    public Buffer readerIndex(final int readerIndex) {
        if (readerIndex < 0 || readerIndex > writerIndex)
            throw new IndexOutOfBoundsException("readerIndex: " + readerIndex);
        this.readerIndex = readerIndex;
        return this;
    }

    @Override
    public int writerIndex() {
        return writerIndex;
    }

    @Override
    public Buffer writerIndex(final int writerIndex) {
        if (writerIndex < readerIndex)
            throw new IndexOutOfBoundsException("writerIndex: " + writerIndex);
        ensureWritable(writerIndex - this.writerIndex);
        this.writerIndex = writerIndex;
        return this;
    }

    @Override
    public Buffer markWriterIndex() {
        this.markedWriterIndex = writerIndex;
        return this;
    }

    @Override
    public Buffer resetWriterIndex() {
        this.writerIndex = markedWriterIndex;
        return this;
    }

    @Override
    public int capacity() {
        return array.length;
    }

    @Override
    public boolean isDirect() {
        return false;
    }

    @Override
    public boolean readBoolean() {
        return readByte() != 0;
    }

    @Override
    public byte readByte() {
        checkReadable(1);
        return array[readerIndex++];
    }

    @Override
    public short readShort() {
        checkReadable(2);
        return (short) (((array[readerIndex++] & 0xFF) << 8) | (array[readerIndex++] & 0xFF));
    }

    @Override
    public int readInt() {
        checkReadable(4);
        return ((array[readerIndex++] & 0xFF) << 24) |
                ((array[readerIndex++] & 0xFF) << 16) |
                ((array[readerIndex++] & 0xFF) << 8) |
                (array[readerIndex++] & 0xFF);
    }

    @Override
    public long readLong() {
        checkReadable(8);
        long value = 0;
        for (int i = 0; i < 8; i++)
            value = (value << 8) | (array[readerIndex++] & 0xFF);
        return value;
    }

    @Override
    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public Buffer readBytes(final byte[] destination) {
        return readBytes(destination, 0, destination.length);
    }

    @Override
    public Buffer readBytes(final byte[] destination, final int dstIndex, final int length) {
        checkReadable(length);
        System.arraycopy(array, readerIndex, destination, dstIndex, length);
        readerIndex += length;
        return this;
    }

    @Override
    public Buffer readBytes(final ByteBuffer dst) {
        final int length = dst.remaining();
        checkReadable(length);
        dst.put(array, readerIndex, length);
        readerIndex += length;
        return this;
    }

    @Override
    public Buffer readBytes(final OutputStream out, final int length) throws IOException {
        checkReadable(length);
        out.write(array, readerIndex, length);
        readerIndex += length;
        return this;
    }

    @Override
    public Buffer writeBoolean(final boolean value) {
        return writeByte(value ? 1 : 0);
    }

    @Override
    public Buffer writeByte(final int value) {
        ensureWritable(1);
        array[writerIndex++] = (byte) value;
        return this;
    }

    @Override
    public Buffer writeShort(final int value) {
        ensureWritable(2);
        array[writerIndex++] = (byte) (value >>> 8);
        array[writerIndex++] = (byte) value;
        return this;
    }

    @Override
    public Buffer writeInt(final int value) {
        ensureWritable(4);
        array[writerIndex++] = (byte) (value >>> 24);
        array[writerIndex++] = (byte) (value >>> 16);
        array[writerIndex++] = (byte) (value >>> 8);
        array[writerIndex++] = (byte) value;
        return this;
    }

    @Override
    public Buffer writeLong(final long value) {
        ensureWritable(8);
        for (int i = 56; i >= 0; i -= 8)
            array[writerIndex++] = (byte) (value >>> i);
        return this;
    }

    @Override
    public Buffer writeFloat(final float value) {
        return writeInt(Float.floatToIntBits(value));
    }

    @Override
    public Buffer writeDouble(final double value) {
        return writeLong(Double.doubleToLongBits(value));
    }

    @Override
    public Buffer writeBytes(final byte[] src) {
        return writeBytes(src, 0, src.length);
    }

    @Override
    public Buffer writeBytes(final ByteBuffer src) {
        final int length = src.remaining();
        ensureWritable(length);
        src.get(array, writerIndex, length);
        writerIndex += length;
        return this;
    }

    @Override
    public Buffer writeBytes(final byte[] src, final int srcIndex, final int length) {
        ensureWritable(length);
        System.arraycopy(src, srcIndex, array, writerIndex, length);
        writerIndex += length;
        return this;
    }

    @Override
    public boolean release() {
        return --referenceCount <= 0;
    }

    @Override
    public Buffer retain() {
        referenceCount++;
        return this;
    }

    @Override
    public int referenceCount() {
        return referenceCount;
    }

    @Override
    public int nioBufferCount() {
        return 1;
    }

    @Override
    public ByteBuffer[] nioBuffers() {
        return new ByteBuffer[] { nioBuffer() };
    }

    @Override
    public ByteBuffer[] nioBuffers(final int index, final int length) {
        return new ByteBuffer[] { nioBuffer(index, length) };
    }

    @Override
    public ByteBuffer nioBuffer() {
        return nioBuffer(readerIndex, readableBytes());
    }

    @Override
    public ByteBuffer nioBuffer(final int index, final int length) {
        return ByteBuffer.wrap(Arrays.copyOfRange(array, index, index + length));
    }

    @Override
    public Buffer getBytes(final int index, final byte[] dst) {
        System.arraycopy(array, index, dst, 0, dst.length);
        return this;
    }
}
