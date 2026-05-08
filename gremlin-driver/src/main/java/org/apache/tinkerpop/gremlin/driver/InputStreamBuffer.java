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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/**
 * A read-only {@link Buffer} implementation backed by a {@link DataInputStream}. Provides sequential read access
 * over a continuous stream, allowing {@code readChunk()} to consume data that spans multiple HTTP response chunks.
 * Write operations are unsupported.
 */
final class InputStreamBuffer implements Buffer {

    private final DataInputStream in;
    private int readerIndex;
    private boolean eof;

    InputStreamBuffer(final InputStream inputStream) {
        this.in = new DataInputStream(inputStream);
    }

    /**
     * Returns a positive value unless EOF has been detected. Since this buffer is backed by a blocking stream,
     * the exact number of available bytes cannot be determined without blocking. Callers should rely on
     * read methods blocking until data is available rather than using this for precise flow control.
     */
    @Override
    public int readableBytes() {
        return eof ? 0 : 1;
    }

    @Override
    public int readerIndex() {
        return readerIndex;
    }

    @Override
    public Buffer readerIndex(final int readerIndex) {
        throw new UnsupportedOperationException("InputStreamBuffer does not support readerIndex repositioning");
    }

    @Override public int writerIndex() { return Integer.MAX_VALUE; }
    @Override public Buffer writerIndex(final int writerIndex) { throw new UnsupportedOperationException(); }
    @Override public Buffer markWriterIndex() { throw new UnsupportedOperationException(); }
    @Override public Buffer resetWriterIndex() { throw new UnsupportedOperationException(); }
    @Override public int capacity() { return Integer.MAX_VALUE; }
    @Override public boolean isDirect() { return false; }

    @Override
    public boolean readBoolean() {
        return readByte() != 0;
    }

    @Override
    public byte readByte() {
        try {
            final byte b = in.readByte();
            readerIndex++;
            return b;
        } catch (final EOFException e) {
            eof = true;
            throw new IndexOutOfBoundsException("End of stream reached");
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public short readShort() {
        try {
            final short v = in.readShort();
            readerIndex += 2;
            return v;
        } catch (final EOFException e) {
            eof = true;
            throw new IndexOutOfBoundsException("End of stream reached");
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int readInt() {
        try {
            final int v = in.readInt();
            readerIndex += 4;
            return v;
        } catch (final EOFException e) {
            eof = true;
            throw new IndexOutOfBoundsException("End of stream reached");
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long readLong() {
        try {
            final long v = in.readLong();
            readerIndex += 8;
            return v;
        } catch (final EOFException e) {
            eof = true;
            throw new IndexOutOfBoundsException("End of stream reached");
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public float readFloat() {
        try {
            final float v = in.readFloat();
            readerIndex += 4;
            return v;
        } catch (final EOFException e) {
            eof = true;
            throw new IndexOutOfBoundsException("End of stream reached");
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public double readDouble() {
        try {
            final double v = in.readDouble();
            readerIndex += 8;
            return v;
        } catch (final EOFException e) {
            eof = true;
            throw new IndexOutOfBoundsException("End of stream reached");
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Buffer readBytes(final byte[] destination) {
        return readBytes(destination, 0, destination.length);
    }

    @Override
    public Buffer readBytes(final byte[] destination, final int dstIndex, final int length) {
        try {
            in.readFully(destination, dstIndex, length);
            readerIndex += length;
            return this;
        } catch (final EOFException e) {
            eof = true;
            throw new IndexOutOfBoundsException("End of stream reached");
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Buffer readBytes(final ByteBuffer dst) {
        final int length = dst.remaining();
        final byte[] tmp = new byte[length];
        readBytes(tmp);
        dst.put(tmp);
        return this;
    }

    @Override
    public Buffer readBytes(final OutputStream out, final int length) throws IOException {
        final byte[] tmp = new byte[length];
        readBytes(tmp);
        out.write(tmp);
        return this;
    }

    // Write operations are unsupported
    @Override public Buffer writeBoolean(final boolean value) { throw new UnsupportedOperationException(); }
    @Override public Buffer writeByte(final int value) { throw new UnsupportedOperationException(); }
    @Override public Buffer writeShort(final int value) { throw new UnsupportedOperationException(); }
    @Override public Buffer writeInt(final int value) { throw new UnsupportedOperationException(); }
    @Override public Buffer writeLong(final long value) { throw new UnsupportedOperationException(); }
    @Override public Buffer writeFloat(final float value) { throw new UnsupportedOperationException(); }
    @Override public Buffer writeDouble(final double value) { throw new UnsupportedOperationException(); }
    @Override public Buffer writeBytes(final byte[] src) { throw new UnsupportedOperationException(); }
    @Override public Buffer writeBytes(final ByteBuffer src) { throw new UnsupportedOperationException(); }
    @Override public Buffer writeBytes(final byte[] src, final int srcIndex, final int length) { throw new UnsupportedOperationException(); }

    @Override public boolean release() { return true; }
    @Override public Buffer retain() { return this; }
    @Override public int referenceCount() { return 1; }
    @Override public int nioBufferCount() { return 0; }
    @Override public ByteBuffer[] nioBuffers() { throw new UnsupportedOperationException(); }
    @Override public ByteBuffer[] nioBuffers(final int index, final int length) { throw new UnsupportedOperationException(); }
    @Override public ByteBuffer nioBuffer() { throw new UnsupportedOperationException(); }
    @Override public ByteBuffer nioBuffer(final int index, final int length) { throw new UnsupportedOperationException(); }
    @Override public Buffer getBytes(final int index, final byte[] dst) { throw new UnsupportedOperationException(); }

    boolean isEof() {
        return eof;
    }
}
