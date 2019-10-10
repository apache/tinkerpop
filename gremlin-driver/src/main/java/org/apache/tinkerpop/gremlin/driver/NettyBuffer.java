/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Represents a {@link Buffer} backed by Netty's {@link ByteBuf}.
 */
final class NettyBuffer implements Buffer {
    private final ByteBuf buffer;

    /**
     * Creates a new instance.
     * @param buffer The buffer to wrap.
     */
    NettyBuffer(ByteBuf buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("buffer can't be null");
        }

        this.buffer = buffer;
    }

    @Override
    public int readableBytes() {
        return this.buffer.readableBytes();
    }

    @Override
    public int readerIndex() {
        return this.buffer.readerIndex();
    }

    @Override
    public Buffer readerIndex(int readerIndex) {
        this.buffer.readerIndex(readerIndex);
        return this;
    }

    @Override
    public int writerIndex() {
        return this.buffer.writerIndex();
    }

    @Override
    public Buffer writerIndex(int writerIndex) {
        this.buffer.writerIndex(writerIndex);
        return this;
    }

    @Override
    public Buffer markWriterIndex() {
        this.buffer.markWriterIndex();
        return this;
    }

    @Override
    public Buffer resetWriterIndex() {
        this.buffer.resetWriterIndex();
        return this;
    }

    @Override
    public int capacity() {
        return this.buffer.capacity();
    }

    @Override
    public boolean isDirect() {
        return this.buffer.isDirect();
    }

    @Override
    public boolean readBoolean() {
        return this.buffer.readBoolean();
    }

    @Override
    public byte readByte() {
        return this.buffer.readByte();
    }

    @Override
    public short readShort() {
        return this.buffer.readShort();
    }

    @Override
    public int readInt() {
        return this.buffer.readInt();
    }

    @Override
    public long readLong() {
        return this.buffer.readLong();
    }

    @Override
    public float readFloat() {
        return this.buffer.readFloat();
    }

    @Override
    public double readDouble() {
        return this.buffer.readDouble();
    }

    @Override
    public Buffer readBytes(byte[] destination) {
        this.buffer.readBytes(destination);
        return this;
    }

    @Override
    public Buffer readBytes(byte[] destination, int dstIndex, int length) {
        this.buffer.readBytes(destination, dstIndex, length);
        return this;
    }

    @Override
    public Buffer readBytes(ByteBuffer dst) {
        this.buffer.readBytes(dst);
        return this;
    }

    @Override
    public Buffer readBytes(OutputStream out, int length) throws IOException {
        this.buffer.readBytes(out, length);
        return this;
    }

    @Override
    public Buffer writeBoolean(boolean value) {
        this.buffer.writeBoolean(value);
        return this;
    }

    @Override
    public Buffer writeByte(int value) {
        this.buffer.writeByte(value);
        return this;
    }

    @Override
    public Buffer writeShort(int value) {
        this.buffer.writeShort(value);
        return this;
    }

    @Override
    public Buffer writeInt(int value) {
        this.buffer.writeInt(value);
        return this;
    }

    @Override
    public Buffer writeLong(long value) {
        this.buffer.writeLong(value);
        return this;
    }

    @Override
    public Buffer writeFloat(float value) {
        this.buffer.writeFloat(value);
        return this;
    }

    @Override
    public Buffer writeDouble(double value) {
        this.buffer.writeDouble(value);
        return this;
    }

    @Override
    public Buffer writeBytes(byte[] src) {
        this.buffer.writeBytes(src);
        return this;
    }

    @Override
    public Buffer writeBytes(ByteBuffer src) {
        this.buffer.writeBytes(src);
        return this;
    }

    @Override
    public Buffer writeBytes(byte[] src, int srcIndex, int length) {
        this.buffer.writeBytes(src, srcIndex, length);
        return this;
    }

    @Override
    public boolean release() {
        return this.buffer.release();
    }

    @Override
    public Buffer retain() {
        this.buffer.retain();
        return this;
    }

    @Override
    public int referenceCount() {
        return this.buffer.refCnt();
    }

    @Override
    public ByteBuffer[] nioBuffers() {
        return this.buffer.nioBuffers();
    }

    @Override
    public ByteBuffer nioBuffer() {
        return this.buffer.nioBuffer();
    }

    @Override
    public Buffer getBytes(int index, byte[] dst) {
        this.buffer.getBytes(index, dst);
        return this;
    }
}
