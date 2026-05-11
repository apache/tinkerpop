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
package org.apache.tinkerpop.gremlin.driver.stream;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A read-only {@link Buffer} implementation backed by a blocking {@link InputStream} via {@link DataInputStream}.
 * Supports only sequential read operations — all write, random-access, and NIO methods throw
 * {@link UnsupportedOperationException}.
 * <p>
 * This allows the existing {@code TypeSerializer} implementations (which only use sequential reads) to work
 * unchanged over a streaming HTTP response body.
 */
public class InputStreamBuffer implements Buffer {

    private final DataInputStream in;
    private int bytesRead;

    public InputStreamBuffer(final InputStream inputStream) {
        this.in = new DataInputStream(inputStream);
    }

    @Override
    public boolean readBoolean() {
        try {
            final boolean v = in.readBoolean();
            bytesRead += 1;
            return v;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte readByte() {
        try {
            final byte v = in.readByte();
            bytesRead += 1;
            return v;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public short readShort() {
        try {
            final short v = in.readShort();
            bytesRead += 2;
            return v;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInt() {
        try {
            final int v = in.readInt();
            bytesRead += 4;
            return v;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readLong() {
        try {
            final long v = in.readLong();
            bytesRead += 8;
            return v;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public float readFloat() {
        try {
            final float v = in.readFloat();
            bytesRead += 4;
            return v;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double readDouble() {
        try {
            final double v = in.readDouble();
            bytesRead += 8;
            return v;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Buffer readBytes(final byte[] destination) {
        try {
            in.readFully(destination);
            bytesRead += destination.length;
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Buffer readBytes(final byte[] destination, final int dstIndex, final int length) {
        try {
            in.readFully(destination, dstIndex, length);
            bytesRead += length;
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Buffer readBytes(final ByteBuffer dst) {
        try {
            final byte[] tmp = new byte[dst.remaining()];
            in.readFully(tmp);
            dst.put(tmp);
            bytesRead += tmp.length;
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Buffer readBytes(final OutputStream out, final int length) throws IOException {
        final byte[] tmp = new byte[length];
        in.readFully(tmp);
        out.write(tmp);
        bytesRead += length;
        return this;
    }

    @Override
    public int readerIndex() {
        return bytesRead;
    }

    @Override
    public int readableBytes() {
        throw new UnsupportedOperationException("readableBytes() is not supported on a streaming Buffer");
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
        return false;
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
        try {
            in.close();
        } catch (IOException e) {
            // best-effort close
        }
        return true;
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
