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
package org.apache.tinkerpop.gremlin.driver.ser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.BufferFactory;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Represents a factory to create {@link Buffer} instances from wrapped {@link ByteBuf} instances.
 */
public class NettyBufferFactory implements BufferFactory<ByteBuf> {
    @Override
    public Buffer create(final ByteBuf value) {
        return new NettyBuffer(value);
    }

    @Override
    public Buffer wrap(final ByteBuffer value) {
        return create(Unpooled.wrappedBuffer(value));
    }

    private static ByteBuf getFromIndex(final Buffer buffer, final int index) {
        if (buffer.nioBufferCount() == 1) {
            // Heap and direct buffers usually take a single buffer
            // It will create a new ByteBuf using the same backing byte array
            return Unpooled.wrappedBuffer(buffer.nioBuffer(index, buffer.capacity() - index));
        }

        // Use a wrapper or composite buffer
        return Unpooled.wrappedBuffer(buffer.nioBuffers(index, buffer.capacity() - index));
    }

    /**
     * Utility method to allow reading from the underlying bytes using a Netty {@link ByteBuf} instance for
     * interoperability, advancing the reader index of the {@link Buffer} after the consumer is called.
     *
     * Note that the {@link ByteBuf} used by the consumer should not be released by the caller.
     * In case the provided {@link Buffer} instance is not a {@link NettyBuffer}, it will create a {@link ByteBuf}
     * wrapper for the consumer to use, releasing it after use.
     */
    public static void readRaw(final Buffer buffer, final Consumer<ByteBuf> consumer) {
        if (buffer instanceof NettyBuffer) {
            consumer.accept(((NettyBuffer)buffer).getUnderlyingBuffer());
            return;
        }

        // Create a new ByteBuf as a wrapper
        final int initialIndex = buffer.readerIndex();
        final ByteBuf newBuffer = getFromIndex(buffer, initialIndex);

        try {
            // Invoke the consumer to read from the ByteBuf
            consumer.accept(newBuffer);

            // Advance the reader index of the Buffer implementation
            buffer.readerIndex(initialIndex + newBuffer.readerIndex());
        } finally {
            newBuffer.release();
        }
    }

    /**
     * Allows writing from the underlying bytes using a Netty {@link ByteBuf} instance for interoperability,
     * advancing the writer index of the {@link Buffer} after the consumer is called.
     *
     * Note that the {@link ByteBuf} used by the consumer should not be released by the caller.
     * In case the provided {@link Buffer} instance is not a {@link NettyBuffer}, it will create a {@link ByteBuf}
     * wrapper for the consumer to use, releasing it after use.
     */
    public static void writeRaw(final Buffer buffer, final Consumer<ByteBuf> consumer) {
        if (buffer instanceof NettyBuffer) {
            consumer.accept(((NettyBuffer)buffer).getUnderlyingBuffer());
            return;
        }

        // Create a new ByteBuf as a wrapper
        final int initialIndex = buffer.writerIndex();
        final ByteBuf newBuffer = getFromIndex(buffer, initialIndex);

        // Set writer index to 0
        newBuffer.writerIndex(0);

        try {
            // Invoke the consumer to read from the ByteBuf
            consumer.accept(newBuffer);

            // Advance the reader index of the Buffer implementation
            buffer.writerIndex(initialIndex + newBuffer.writerIndex());
        } finally {
            newBuffer.release();
        }
    }
}
