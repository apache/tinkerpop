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
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.driver.stream.ByteBufQueueInputStream;
import org.apache.tinkerpop.gremlin.driver.stream.InputStreamBuffer;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class InputStreamBufferTest {

    @Test
    public void shouldReadPrimitivesThroughInputStreamBuffer() throws Exception {
        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        final io.netty.buffer.ByteBuf buf = Unpooled.buffer();
        buf.writeByte(42);
        buf.writeInt(12345);
        buf.writeLong(9876543210L);
        buf.writeFloat(3.14f);
        buf.writeDouble(2.718281828);
        buf.writeShort(256);
        buf.writeBoolean(true);
        stream.offer(buf);
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        assertEquals(42, buffer.readByte());
        assertEquals(12345, buffer.readInt());
        assertEquals(9876543210L, buffer.readLong());
        assertEquals(3.14f, buffer.readFloat(), 0.001f);
        assertEquals(2.718281828, buffer.readDouble(), 0.000001);
        assertEquals(256, buffer.readShort());
        assertTrue(buffer.readBoolean());
    }

    @Test
    public void shouldReadBytesArray() throws Exception {
        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(Unpooled.wrappedBuffer(new byte[]{10, 20, 30}));
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        final byte[] dest = new byte[3];
        buffer.readBytes(dest);
        assertArrayEquals(new byte[]{10, 20, 30}, dest);
    }

    @Test
    public void shouldTrackReaderIndex() throws Exception {
        final ByteBufQueueInputStream stream = new ByteBufQueueInputStream();
        stream.offer(Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}));
        stream.signalEndOfStream();

        final InputStreamBuffer buffer = new InputStreamBuffer(stream);
        assertEquals(0, buffer.readerIndex());
        buffer.readByte();
        assertEquals(1, buffer.readerIndex());
        buffer.readInt();
        assertEquals(5, buffer.readerIndex());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnReadableBytes() {
        new InputStreamBuffer(new ByteBufQueueInputStream()).readableBytes();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnWriteInt() {
        new InputStreamBuffer(new ByteBufQueueInputStream()).writeInt(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnNioBuffer() {
        new InputStreamBuffer(new ByteBufQueueInputStream()).nioBuffer();
    }

    @Test
    public void shouldRoundTripBoundaryPrimitiveValues() throws Exception {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(bos);
        dos.writeBoolean(false);
        dos.writeByte(-1);
        dos.writeShort(Short.MIN_VALUE);
        dos.writeInt(Integer.MIN_VALUE);
        dos.writeLong(Long.MAX_VALUE);
        dos.writeFloat(Float.NaN);
        dos.writeDouble(Double.NEGATIVE_INFINITY);

        final InputStreamBuffer buffer = bufferOf(bos.toByteArray());
        assertFalse(buffer.readBoolean());
        assertEquals((byte) -1, buffer.readByte());
        assertEquals(Short.MIN_VALUE, buffer.readShort());
        assertEquals(Integer.MIN_VALUE, buffer.readInt());
        assertEquals(Long.MAX_VALUE, buffer.readLong());
        assertTrue(Float.isNaN(buffer.readFloat()));
        assertEquals(Double.NEGATIVE_INFINITY, buffer.readDouble(), 0.0);
        // 1 + 1 + 2 + 4 + 8 + 4 + 8 = 28 bytes consumed
        assertEquals(28, buffer.readerIndex());
    }

    @Test
    public void shouldReadBytesIntoArrayWithOffsetAndLength() {
        final InputStreamBuffer buffer = bufferOf(new byte[]{10, 20, 30, 40});
        final byte[] dest = new byte[6];
        buffer.readBytes(dest, 1, 4);
        assertArrayEquals(new byte[]{0, 10, 20, 30, 40, 0}, dest);
        assertEquals(4, buffer.readerIndex());
    }

    @Test
    public void shouldReadBytesIntoByteBuffer() {
        final InputStreamBuffer buffer = bufferOf(new byte[]{1, 2, 3, 4});
        final ByteBuffer dst = ByteBuffer.allocate(4);
        buffer.readBytes(dst);
        assertArrayEquals(new byte[]{1, 2, 3, 4}, dst.array());
        assertEquals(4, buffer.readerIndex());
    }

    @Test
    public void shouldReadBytesIntoOutputStream() throws Exception {
        final InputStreamBuffer buffer = bufferOf(new byte[]{5, 6, 7});
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        buffer.readBytes(out, 3);
        assertArrayEquals(new byte[]{5, 6, 7}, out.toByteArray());
        assertEquals(3, buffer.readerIndex());
    }

    @Test
    public void shouldWrapIOExceptionFromUnderlyingStreamOnRead() {
        final List<BufferOp> reads = Arrays.asList(
                InputStreamBuffer::readBoolean,
                InputStreamBuffer::readByte,
                InputStreamBuffer::readShort,
                InputStreamBuffer::readInt,
                InputStreamBuffer::readLong,
                InputStreamBuffer::readFloat,
                InputStreamBuffer::readDouble,
                b -> b.readBytes(new byte[1]),
                b -> b.readBytes(new byte[1], 0, 1),
                b -> b.readBytes(ByteBuffer.allocate(1)));

        for (final BufferOp read : reads) {
            // an empty stream forces DataInputStream to throw EOFException, which the class wraps
            final InputStreamBuffer buffer = bufferOf(new byte[0]);
            try {
                read.apply(buffer);
                fail("expected RuntimeException wrapping an IOException");
            } catch (RuntimeException e) {
                assertTrue("cause should be an IOException but was " + e.getCause(),
                        e.getCause() instanceof IOException);
            }
        }
    }

    @Test(expected = IOException.class)
    public void shouldPropagateIOExceptionFromReadBytesToOutputStream() throws Exception {
        // this overload declares throws IOException, so the EOFException is not wrapped
        bufferOf(new byte[0]).readBytes(new ByteArrayOutputStream(), 4);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOnGetBytes() {
        bufferOf(new byte[0]).getBytes(0, new byte[1]);
    }

    @Test
    public void shouldNotBeDirect() {
        assertFalse(bufferOf(new byte[0]).isDirect());
    }

    @Test
    public void shouldThrowUnsupportedOnWriteAndRandomAccessOperations() {
        final List<BufferOp> unsupported = Arrays.asList(
                b -> b.writeBoolean(true),
                b -> b.writeByte(1),
                b -> b.writeShort(1),
                b -> b.writeLong(1L),
                b -> b.writeFloat(1f),
                b -> b.writeDouble(1d),
                b -> b.writeBytes(new byte[1]),
                b -> b.writeBytes(ByteBuffer.allocate(1)),
                b -> b.writeBytes(new byte[1], 0, 1),
                InputStreamBuffer::writerIndex,
                b -> b.writerIndex(0),
                InputStreamBuffer::markWriterIndex,
                InputStreamBuffer::resetWriterIndex,
                b -> b.readerIndex(0),
                InputStreamBuffer::capacity,
                InputStreamBuffer::retain,
                InputStreamBuffer::referenceCount,
                InputStreamBuffer::nioBufferCount,
                InputStreamBuffer::nioBuffers,
                b -> b.nioBuffers(0, 1),
                b -> b.nioBuffer(0, 1));

        for (final BufferOp op : unsupported) {
            final InputStreamBuffer buffer = bufferOf(new byte[0]);
            try {
                op.apply(buffer);
                fail("expected UnsupportedOperationException");
            } catch (UnsupportedOperationException expected) {
                // expected
            }
        }
    }

    @Test
    public void shouldReleaseAndCloseUnderlyingStream() {
        final AtomicBoolean closed = new AtomicBoolean(false);
        final InputStream in = new ByteArrayInputStream(new byte[]{1, 2, 3}) {
            @Override
            public void close() throws IOException {
                closed.set(true);
                super.close();
            }
        };

        final InputStreamBuffer buffer = new InputStreamBuffer(in);
        assertTrue(buffer.release());
        assertTrue("underlying stream should be closed by release()", closed.get());
    }

    @Test
    public void shouldReturnTrueFromReleaseEvenWhenCloseFails() {
        final InputStream in = new InputStream() {
            @Override
            public int read() {
                return -1;
            }

            @Override
            public void close() throws IOException {
                throw new IOException("close failure should be swallowed");
            }
        };

        // release() performs a best-effort close and must still report success
        assertTrue(new InputStreamBuffer(in).release());
    }

    private static InputStreamBuffer bufferOf(final byte[] bytes) {
        return new InputStreamBuffer(new ByteArrayInputStream(bytes));
    }

    @FunctionalInterface
    private interface BufferOp {
        void apply(InputStreamBuffer buffer);
    }
}
