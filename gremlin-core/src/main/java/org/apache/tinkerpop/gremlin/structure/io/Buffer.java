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
package org.apache.tinkerpop.gremlin.structure.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Represents an abstract view for one or more primitive byte arrays and NIO buffers.
 */
public interface Buffer {

    /**
     * Returns the number of readable bytes.
     */
    int readableBytes();

    /**
     * Returns the reader index of this buffer.
     */
    int readerIndex();

    /**
     * Sets the reader index of this buffer.
     *
     * @throws IndexOutOfBoundsException
     *         if its out of bounds.
     */
    Buffer readerIndex(int readerIndex);

    /**
     * Returns the writer index of this buffer.
     */
    int writerIndex();

    /**
     * Sets the writer index of this buffer.
     */
    Buffer writerIndex(int writerIndex);

    /**
     * Marks the current writer index in this buffer.
     */
    Buffer markWriterIndex();

    /**
     * Repositions the current writer index to the marked index in this buffer.
     */
    Buffer resetWriterIndex();

    /**
     * Returns the number of bytes (octets) this buffer can contain.
     */
    int capacity();

    /**
     * Returns {@code true} if and only if this buffer is backed by an
     * NIO direct buffer.
     */
    boolean isDirect();

    /**
     * Gets a boolean and advances the reader index.
     */
    boolean readBoolean();

    /**
     * Gets a byte and advances the reader index.
     */
    byte readByte();

    /**
     * Gets a 16-bit short integer and advances the reader index.
     */
    short readShort();

    /**
     * Gets a 32-bit integer at the current index and advances the reader index.
     */
    int readInt();

    /**
     * Gets a 64-bit integer  and advances the reader index.
     */
    long readLong();

    /**
     * Gets a 32-bit floating point number and advances the reader index.
     */
    float readFloat();

    /**
     * Gets a 64-bit floating point number and advances the reader index.
     */
    double readDouble();

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current reader index and advances the reader index.
     */
    Buffer readBytes(byte[] destination);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current reader index and advances the reader index.
     *
     * @param destination The destination buffer
     * @param dstIndex the first index of the destination
     * @param length   the number of bytes to transfer
     */
    Buffer readBytes(byte[] destination, int dstIndex, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the current reader index until the destination's position
     * reaches its limit, and advances the reader index.
     */
    Buffer readBytes(ByteBuffer dst);

    /**
     * Transfers this buffer's data to the specified stream starting at the
     * current reader index and advances the index.
     *
     * @param length the number of bytes to transfer
     *
     * @throws IOException
     *         if the specified stream threw an exception during I/O
     */
    Buffer readBytes(OutputStream out, int length) throws IOException;

    /**
     * Sets the specified boolean at the current writer index and advances the index.
     */
    Buffer writeBoolean(boolean value);

    /**
     * Sets the specified byte at the current writer index and advances the index.
     */
    Buffer writeByte(int value);

    /**
     * Sets the specified 16-bit short integer at the current writer index and advances the index.
     */
    Buffer writeShort(int value);

    /**
     * Sets the specified 32-bit integer at the current writer index and advances the index.
     */
    Buffer writeInt(int value);

    /**
     * Sets the specified 64-bit long integer at the current writer index and advances the index.
     */
    Buffer writeLong(long value);

    /**
     * Sets the specified 32-bit floating point number at the current writer index and advances the index.
     */
    Buffer writeFloat(float value);

    /**
     * Sets the specified 64-bit floating point number at the current writer index and advances the index.
     */
    Buffer writeDouble(double value);

    /**
     * Transfers the specified source array's data to this buffer starting at the current writer index
     * and advances the index.
     */
    Buffer writeBytes(byte[] src);

    /**
     * Transfers the specified source byte data to this buffer starting at the current writer index
     * and advances the index.
     */
    Buffer writeBytes(ByteBuffer src);

    /**
     * Transfers the specified source array's data to this buffer starting at the current writer index
     * and advances the index.
     */
    Buffer writeBytes(byte[] src, int srcIndex, int length);

    /**
     * Decreases the reference count by {@code 1} and deallocates this object if the reference count reaches at
     * {@code 0}.
     */
    boolean release();

    /**
     * Increases the reference count by {@code 1}.
     */
    Buffer retain();

    /**
     * Returns the reference count of this object.
     */
    int referenceCount();

    /**
     * Returns the maximum number of NIO {@link ByteBuffer}s that consist this buffer.
     */
    int nioBufferCount();

    /**
     * Exposes this buffer's readable bytes as NIO ByteBuffer's instances.
     */
    ByteBuffer[] nioBuffers();

    /**
     * Exposes this buffer's readable bytes as NIO ByteBuffer's instances.
     */
    ByteBuffer[] nioBuffers(int index, int length);

    /**
     * Exposes this buffer's readable bytes as a NIO {@link ByteBuffer}. The returned buffer
     * either share or contains the copied content of this buffer, while changing the position
     * and limit of the returned NIO buffer does not affect the indexes and marks of this buffer.
     */
    ByteBuffer nioBuffer();

    /**
     * Exposes this buffer's sub-region as an NIO {@link ByteBuffer}.
     */
    ByteBuffer nioBuffer(int index, int length);

    /**
     * Transfers this buffer's data to the specified destination starting at
     * the specified absolute {@code index}.
     * This method does not modify reader or writer indexes.
     */
    Buffer getBytes(int index, byte[] dst);
}
