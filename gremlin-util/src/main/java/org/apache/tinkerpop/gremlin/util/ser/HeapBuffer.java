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
package org.apache.tinkerpop.gremlin.util.ser;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Buffer} implementation backed by a growable {@code byte[]} that does not depend on Netty.
 */
final class HeapBuffer implements Buffer {
    private byte[] data;
    private int readerIndex;
    private int writerIndex;
    private int markedWriterIndex;
    private final AtomicInteger refCount = new AtomicInteger(1);

    HeapBuffer(final int initialCapacity) {
        this.data = new byte[initialCapacity];
    }

    private HeapBuffer(final byte[] data, final int writerIndex) {
        this.data = data;
        this.writerIndex = writerIndex;
    }

    static HeapBuffer wrap(final ByteBuffer nioBuffer) {
        final int len = nioBuffer.remaining();
        final byte[] bytes = new byte[len];
        nioBuffer.get(bytes);
        return new HeapBuffer(bytes, len);
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
        if (readerIndex < 0 || readerIndex > writerIndex) {
            throw new IndexOutOfBoundsException("readerIndex: " + readerIndex + " (expected: 0 <= readerIndex <= writerIndex(" + writerIndex + "))");
        }
        this.readerIndex = readerIndex;
        return this;
    }

    @Override
    public int writerIndex() {
        return writerIndex;
    }

    @Override
    public Buffer writerIndex(final int writerIndex) {
        if (writerIndex < readerIndex) {
            throw new IndexOutOfBoundsException("writerIndex: " + writerIndex + " (expected: readerIndex(" + readerIndex + ") <= writerIndex)");
        }
        ensureCapacity(writerIndex);
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
        return data.length;
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
        return data[readerIndex++];
    }

    @Override
    public short readShort() {
        checkReadable(2);
        final short v = (short) ((data[readerIndex] & 0xFF) << 8 | (data[readerIndex + 1] & 0xFF));
        readerIndex += 2;
        return v;
    }

    @Override
    public int readInt() {
        checkReadable(4);
        final int v = (data[readerIndex] & 0xFF) << 24 |
                      (data[readerIndex + 1] & 0xFF) << 16 |
                      (data[readerIndex + 2] & 0xFF) << 8 |
                      (data[readerIndex + 3] & 0xFF);
        readerIndex += 4;
        return v;
    }

    @Override
    public long readLong() {
        checkReadable(8);
        final long v = ((long) (data[readerIndex] & 0xFF)) << 56 |
                       ((long) (data[readerIndex + 1] & 0xFF)) << 48 |
                       ((long) (data[readerIndex + 2] & 0xFF)) << 40 |
                       ((long) (data[readerIndex + 3] & 0xFF)) << 32 |
                       ((long) (data[readerIndex + 4] & 0xFF)) << 24 |
                       ((long) (data[readerIndex + 5] & 0xFF)) << 16 |
                       ((long) (data[readerIndex + 6] & 0xFF)) << 8 |
                       ((long) (data[readerIndex + 7] & 0xFF));
        readerIndex += 8;
        return v;
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
        System.arraycopy(data, readerIndex, destination, dstIndex, length);
        readerIndex += length;
        return this;
    }

    @Override
    public Buffer readBytes(final ByteBuffer dst) {
        final int length = dst.remaining();
        checkReadable(length);
        dst.put(data, readerIndex, length);
        readerIndex += length;
        return this;
    }

    @Override
    public Buffer readBytes(final OutputStream out, final int length) throws IOException {
        checkReadable(length);
        out.write(data, readerIndex, length);
        readerIndex += length;
        return this;
    }

    @Override
    public Buffer writeBoolean(final boolean value) {
        return writeByte(value ? 1 : 0);
    }

    @Override
    public Buffer writeByte(final int value) {
        ensureCapacity(writerIndex + 1);
        data[writerIndex++] = (byte) value;
        return this;
    }

    @Override
    public Buffer writeShort(final int value) {
        ensureCapacity(writerIndex + 2);
        data[writerIndex++] = (byte) (value >>> 8);
        data[writerIndex++] = (byte) value;
        return this;
    }

    @Override
    public Buffer writeInt(final int value) {
        ensureCapacity(writerIndex + 4);
        data[writerIndex++] = (byte) (value >>> 24);
        data[writerIndex++] = (byte) (value >>> 16);
        data[writerIndex++] = (byte) (value >>> 8);
        data[writerIndex++] = (byte) value;
        return this;
    }

    @Override
    public Buffer writeLong(final long value) {
        ensureCapacity(writerIndex + 8);
        data[writerIndex++] = (byte) (value >>> 56);
        data[writerIndex++] = (byte) (value >>> 48);
        data[writerIndex++] = (byte) (value >>> 40);
        data[writerIndex++] = (byte) (value >>> 32);
        data[writerIndex++] = (byte) (value >>> 24);
        data[writerIndex++] = (byte) (value >>> 16);
        data[writerIndex++] = (byte) (value >>> 8);
        data[writerIndex++] = (byte) value;
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
        ensureCapacity(writerIndex + length);
        src.get(data, writerIndex, length);
        writerIndex += length;
        return this;
    }

    @Override
    public Buffer writeBytes(final byte[] src, final int srcIndex, final int length) {
        ensureCapacity(writerIndex + length);
        System.arraycopy(src, srcIndex, data, writerIndex, length);
        writerIndex += length;
        return this;
    }

    @Override
    public boolean release() {
        return refCount.decrementAndGet() == 0;
    }

    @Override
    public Buffer retain() {
        refCount.incrementAndGet();
        return this;
    }

    @Override
    public int referenceCount() {
        return refCount.get();
    }

    @Override
    public int nioBufferCount() {
        return 1;
    }

    @Override
    public ByteBuffer[] nioBuffers() {
        return new ByteBuffer[]{nioBuffer()};
    }

    @Override
    public ByteBuffer[] nioBuffers(final int index, final int length) {
        return new ByteBuffer[]{nioBuffer(index, length)};
    }

    @Override
    public ByteBuffer nioBuffer() {
        return nioBuffer(readerIndex, readableBytes());
    }

    @Override
    public ByteBuffer nioBuffer(final int index, final int length) {
        return ByteBuffer.wrap(data, index, length).slice();
    }

    @Override
    public Buffer getBytes(final int index, final byte[] dst) {
        System.arraycopy(data, index, dst, 0, dst.length);
        return this;
    }

    private void ensureCapacity(final int minCapacity) {
        if (minCapacity > data.length) {
            int newCapacity = data.length == 0 ? 64 : data.length;
            while (newCapacity < minCapacity) {
                newCapacity <<= 1;
            }
            data = Arrays.copyOf(data, newCapacity);
        }
    }

    private void checkReadable(final int length) {
        if (readerIndex + length > writerIndex) {
            throw new IndexOutOfBoundsException("readerIndex(" + readerIndex + ") + length(" + length + ") exceeds writerIndex(" + writerIndex + ")");
        }
    }
}
