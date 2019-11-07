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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCounted;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class NettyBufferFactoryTest {
    public static final NettyBufferFactory factory = new NettyBufferFactory();
    private static final ByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    private static final List<ByteBuf> rawInstances = new ArrayList<>();

    @AfterClass
    public static void tearDown() {
        rawInstances.forEach(ReferenceCounted::release);
    }

    private static ByteBuf getRaw() {
        final ByteBuf raw = allocator.buffer(256);
        rawInstances.add(raw);
        return raw;
    }

    @Test
    public void shouldReturnAWrapper() {
        final ByteBuf raw = getRaw();
        final Buffer buffer = factory.create(raw);
        assertEquals(raw.refCnt(), buffer.referenceCount());
    }

    @Test
    public void shouldAdvanceWriterAndReaderIndex() {
        final ByteBuf raw = getRaw();
        final Buffer buffer = factory.create(raw);

        final int intValue = 100;
        final long longValue = 2019L;
        final float floatValue = 10.9F;

        assertEquals(0, buffer.writerIndex());
        assertEquals(0, buffer.readerIndex());

        buffer.writeBoolean(true);
        buffer.writeInt(intValue);
        buffer.writeLong(longValue);
        buffer.writeFloat(floatValue);

        assertEquals(17, buffer.writerIndex());
        assertEquals(0, buffer.readerIndex());

        assertTrue(buffer.readBoolean());
        assertEquals(intValue, buffer.readInt());
        assertEquals(longValue, buffer.readLong());
        assertEquals(floatValue, buffer.readFloat(), 0f);

        assertEquals(17, buffer.writerIndex());
        assertEquals(17, buffer.readerIndex());
    }

    @Test
    public void readRawShouldAdvanceReaderIndexAndReleaseIt() {
        final int intValue = 5;
        final FakeBuffer fakeBuffer = new FakeBuffer();
        fakeBuffer.writeInt(intValue);
        assertEquals(4, fakeBuffer.writerIndex());
        assertEquals(0, fakeBuffer.readerIndex());
        final ByteBuf[] captured = new ByteBuf[1];

        NettyBufferFactory.readRaw(fakeBuffer, byteBuf -> {
            assertEquals(intValue, byteBuf.readInt());
            assertNotSame(byteBuf, fakeBuffer.getUnderlyingRaw());
            assertEquals(1, byteBuf.refCnt());
            captured[0] = byteBuf;
        });

        assertEquals(4, fakeBuffer.writerIndex());

        // The reader index advanced
        assertEquals(4, fakeBuffer.readerIndex());

        // Should be released afterwards
        assertEquals(0, captured[0].refCnt());
    }

    @Test
    public void writeRawShouldAdvanceWriterIndexAndReleaseIt() {
        final int intValue1 = 314;
        final int intValue2 = 314;
        final FakeBuffer fakeBuffer = new FakeBuffer();

        fakeBuffer.writeInt(intValue1);
        assertEquals(4, fakeBuffer.writerIndex());
        assertEquals(0, fakeBuffer.readerIndex());
        final ByteBuf[] captured = new ByteBuf[1];

        NettyBufferFactory.writeRaw(fakeBuffer, byteBuf -> {
            byteBuf.writeInt(intValue2);
            assertNotSame(byteBuf, fakeBuffer.getUnderlyingRaw());
            assertEquals(1, byteBuf.refCnt());
            captured[0] = byteBuf;
        });

        // The writer index advanced
        assertEquals(8, fakeBuffer.writerIndex());
        assertEquals(0, fakeBuffer.readerIndex());

        // Should have painted the underlying bytes
        assertEquals(intValue1, fakeBuffer.readInt());
        assertEquals(intValue2, fakeBuffer.readInt());

        // Should be released afterwards
        assertEquals(0, captured[0].refCnt());
    }

    @Test
    public void readRawShouldUseTheSameBufferWhenNettyBuffer() {
        final NettyBuffer wrapperBuffer = new NettyBuffer(allocator.buffer());

        NettyBufferFactory.readRaw(wrapperBuffer, byteBuf -> {
            assertSame(byteBuf, wrapperBuffer.getUnderlyingBuffer());
            assertEquals(1, byteBuf.refCnt());
        });

        // It shouldn't have released it
        assertEquals(1, wrapperBuffer.referenceCount());
    }

    @Test
    public void writeRawShouldUseTheSameBufferWhenNettyBuffer() {
        final NettyBuffer wrapperBuffer = new NettyBuffer(allocator.buffer());

        NettyBufferFactory.writeRaw(wrapperBuffer, byteBuf -> {
            assertSame(byteBuf, wrapperBuffer.getUnderlyingBuffer());
            assertEquals(1, byteBuf.refCnt());
        });

        // It shouldn't have released it
        assertEquals(1, wrapperBuffer.referenceCount());
    }

    /** An incomplete implementation that allows testing */
    class FakeBuffer implements Buffer {
        private final ByteBuf buffer = getRaw();

        FakeBuffer() {

        }

        ByteBuf getUnderlyingRaw() {
            return buffer;
        }

        @Override
        public int readableBytes() {
            return buffer.readableBytes();
        }

        @Override
        public int readerIndex() {
            return buffer.readerIndex();
        }

        @Override
        public Buffer readerIndex(int readerIndex) {
            buffer.readerIndex(readerIndex);
            return this;
        }

        @Override
        public int writerIndex() {
            return buffer.writerIndex();
        }

        @Override
        public Buffer writerIndex(int writerIndex) {
            buffer.writerIndex(writerIndex);
            return this;
        }

        @Override
        public Buffer markWriterIndex() {
            return null;
        }

        @Override
        public Buffer resetWriterIndex() {
            return null;
        }

        @Override
        public int capacity() {
            return buffer.capacity();
        }

        @Override
        public boolean isDirect() {
            return false;
        }

        @Override
        public boolean readBoolean() {
            return false;
        }

        @Override
        public byte readByte() {
            return 0;
        }

        @Override
        public short readShort() {
            return 0;
        }

        @Override
        public int readInt() {
            return buffer.readInt();
        }

        @Override
        public long readLong() {
            return 0;
        }

        @Override
        public float readFloat() {
            return 0;
        }

        @Override
        public double readDouble() {
            return 0;
        }

        @Override
        public Buffer readBytes(byte[] destination) {
            return null;
        }

        @Override
        public Buffer readBytes(byte[] destination, int dstIndex, int length) {
            return null;
        }

        @Override
        public Buffer readBytes(ByteBuffer dst) {
            return null;
        }

        @Override
        public Buffer readBytes(OutputStream out, int length) {
            return null;
        }

        @Override
        public Buffer writeBoolean(boolean value) {
            return null;
        }

        @Override
        public Buffer writeByte(int value) {
            return null;
        }

        @Override
        public Buffer writeShort(int value) {
            return null;
        }

        @Override
        public Buffer writeInt(int value) {
            buffer.writeInt(value);
            return this;
        }

        @Override
        public Buffer writeLong(long value) {
            return null;
        }

        @Override
        public Buffer writeFloat(float value) {
            return null;
        }

        @Override
        public Buffer writeDouble(double value) {
            return null;
        }

        @Override
        public Buffer writeBytes(byte[] src) {
            return null;
        }

        @Override
        public Buffer writeBytes(ByteBuffer src) {
            return null;
        }

        @Override
        public Buffer writeBytes(byte[] src, int srcIndex, int length) {
            return null;
        }

        @Override
        public boolean release() {
            return buffer.release();
        }

        @Override
        public Buffer retain() {
            buffer.retain();
            return this;
        }

        @Override
        public int referenceCount() {
            return buffer.refCnt();
        }

        @Override
        public int nioBufferCount() {
            return buffer.nioBufferCount();
        }

        @Override
        public ByteBuffer[] nioBuffers() {
            return buffer.nioBuffers();
        }

        @Override
        public ByteBuffer[] nioBuffers(int index, int length) {
            return buffer.nioBuffers(index, length);
        }

        @Override
        public ByteBuffer nioBuffer() {
            return buffer.nioBuffer();
        }

        @Override
        public ByteBuffer nioBuffer(int index, int length) {
            return buffer.nioBuffer(index, length);
        }

        @Override
        public Buffer getBytes(int index, byte[] dst) {
            return null;
        }
    }
}
