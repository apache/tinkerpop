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

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link TinkerByteBuffer}, the heap {@code byte[]} implementation of the gremlin-core
 * {@code Buffer} abstraction that backs the GraphBinary storage codec. The byte order and bounds behaviour asserted
 * here are relied on by the on-disk format, so a change that breaks them changes what a store means.
 */
public class TinkerByteBufferTest {

    @Test
    public void shouldRoundTripEveryPrimitive() {
        final TinkerByteBuffer buf = new TinkerByteBuffer();
        buf.writeBoolean(true).writeBoolean(false)
                .writeByte(0x7F).writeShort(-2)
                .writeInt(Integer.MIN_VALUE).writeLong(Long.MAX_VALUE)
                .writeFloat(0.5f).writeDouble(-1.25d);

        assertTrue(buf.readBoolean());
        assertFalse(buf.readBoolean());
        assertEquals(0x7F, buf.readByte());
        assertEquals((short) -2, buf.readShort());
        assertEquals(Integer.MIN_VALUE, buf.readInt());
        assertEquals(Long.MAX_VALUE, buf.readLong());
        assertEquals(0.5f, buf.readFloat(), 0.0f);
        assertEquals(-1.25d, buf.readDouble(), 0.0d);
        assertEquals(0, buf.readableBytes());
    }

    @Test
    public void shouldWriteMultiByteValuesBigEndian() {
        // the on-disk format is big-endian, matching the Netty-backed buffer this replaces, so the byte order is
        // part of the storage contract rather than an implementation detail
        final TinkerByteBuffer buf = new TinkerByteBuffer();
        buf.writeShort(0x0102).writeInt(0x03040506).writeLong(0x0708090A0B0C0D0EL);

        assertArrayEquals(new byte[] {
                0x01, 0x02,
                0x03, 0x04, 0x05, 0x06,
                0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E
        }, buf.toWrittenArray());
    }

    @Test
    public void shouldGrowBeyondInitialCapacity() {
        final TinkerByteBuffer buf = new TinkerByteBuffer(1);
        final byte[] payload = new byte[1000];
        Arrays.fill(payload, (byte) 7);
        buf.writeBytes(payload);

        assertTrue("capacity should have grown to hold the payload", buf.capacity() >= 1000);
        assertEquals(1000, buf.readableBytes());
        assertArrayEquals(payload, buf.toWrittenArray());
    }

    @Test
    public void shouldWrapAnExistingArrayReadyForReading() {
        final byte[] data = { 0x00, 0x00, 0x00, 0x2A };
        final TinkerByteBuffer buf = new TinkerByteBuffer(data);

        assertEquals(4, buf.writerIndex());
        assertEquals(0, buf.readerIndex());
        assertEquals(4, buf.readableBytes());
        assertEquals(42, buf.readInt());
    }

    @Test
    public void shouldTrackReaderAndWriterIndexes() {
        final TinkerByteBuffer buf = new TinkerByteBuffer();
        buf.writeInt(1).writeInt(2);
        assertEquals(8, buf.writerIndex());
        assertEquals(8, buf.readableBytes());

        buf.readInt();
        assertEquals(4, buf.readerIndex());
        assertEquals(4, buf.readableBytes());

        buf.readerIndex(0);
        assertEquals(1, buf.readInt());
    }

    @Test
    public void shouldDistinguishReadableFromWrittenBytes() {
        final TinkerByteBuffer buf = new TinkerByteBuffer();
        buf.writeInt(1).writeInt(2);
        buf.readInt();

        // written covers everything from index 0; readable covers only what is left ahead of the reader
        assertArrayEquals(new byte[] { 0, 0, 0, 1, 0, 0, 0, 2 }, buf.toWrittenArray());
        assertArrayEquals(new byte[] { 0, 0, 0, 2 }, buf.toReadableArray());
        assertEquals("neither view may move the indexes", 4, buf.readerIndex());
    }

    @Test
    public void shouldResetWriterIndexToTheMark() {
        final TinkerByteBuffer buf = new TinkerByteBuffer();
        buf.writeInt(1);
        buf.markWriterIndex();
        buf.writeInt(2);
        assertEquals(8, buf.writerIndex());

        buf.resetWriterIndex();
        assertEquals(4, buf.writerIndex());
        assertArrayEquals(new byte[] { 0, 0, 0, 1 }, buf.toWrittenArray());
    }

    @Test
    public void shouldRejectReadingPastTheWriterIndex() {
        final TinkerByteBuffer buf = new TinkerByteBuffer();
        buf.writeByte(1);
        buf.readByte();

        try {
            buf.readByte();
            fail("expected a read past the writer index to be rejected");
        } catch (IndexOutOfBoundsException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("Not enough readable bytes"));
        }
    }

    @Test
    public void shouldRejectReadingMoreBytesThanRemain() {
        final TinkerByteBuffer buf = new TinkerByteBuffer(new byte[] { 1, 2, 3 });
        try {
            buf.readBytes(new byte[4]);
            fail("expected a bulk read longer than the readable region to be rejected");
        } catch (IndexOutOfBoundsException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("Not enough readable bytes"));
        }
    }

    @Test
    public void shouldRejectAnOutOfRangeReaderIndex() {
        final TinkerByteBuffer buf = new TinkerByteBuffer();
        buf.writeInt(1);
        try {
            buf.readerIndex(5);
            fail("expected a reader index beyond the writer index to be rejected");
        } catch (IndexOutOfBoundsException expected) {
            // expected: the readable region may never extend past what has been written
        }
    }

    @Test
    public void shouldReadBytesIntoAnArraySlice() {
        final TinkerByteBuffer buf = new TinkerByteBuffer(new byte[] { 1, 2, 3, 4 });
        final byte[] dst = new byte[6];
        buf.readBytes(dst, 1, 4);

        assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 0 }, dst);
        assertEquals(0, buf.readableBytes());
    }

    @Test
    public void shouldReadAndWriteThroughNioBuffers() {
        final TinkerByteBuffer buf = new TinkerByteBuffer();
        buf.writeBytes(ByteBuffer.wrap(new byte[] { 9, 8, 7 }));
        assertEquals(3, buf.readableBytes());

        final ByteBuffer dst = ByteBuffer.allocate(3);
        buf.readBytes(dst);
        assertArrayEquals(new byte[] { 9, 8, 7 }, dst.array());
    }

    @Test
    public void shouldReadBytesIntoAnOutputStream() throws Exception {
        final TinkerByteBuffer buf = new TinkerByteBuffer(new byte[] { 4, 5, 6, 7 });
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        buf.readBytes(out, 3);

        assertArrayEquals(new byte[] { 4, 5, 6 }, out.toByteArray());
        assertEquals(1, buf.readableBytes());
    }

    @Test
    public void shouldCopyAbsoluteBytesWithoutMovingIndexes() {
        final TinkerByteBuffer buf = new TinkerByteBuffer(new byte[] { 1, 2, 3, 4 });
        final byte[] dst = new byte[2];
        buf.getBytes(2, dst);

        assertArrayEquals(new byte[] { 3, 4 }, dst);
        assertEquals("an absolute read is positional and must not consume", 0, buf.readerIndex());
    }

    @Test
    public void shouldExposeAnNioViewOfTheReadableRegion() {
        final TinkerByteBuffer buf = new TinkerByteBuffer(new byte[] { 1, 2, 3, 4 });
        buf.readByte();

        assertEquals(1, buf.nioBufferCount());
        final ByteBuffer view = buf.nioBuffer();
        assertArrayEquals(new byte[] { 2, 3, 4 }, view.array());
        assertEquals("the view is a copy, so consuming it must not move the buffer", 3, buf.readableBytes());
        assertArrayEquals(new byte[] { 2, 3 }, buf.nioBuffer(1, 2).array());
        assertEquals(1, buf.nioBuffers().length);
    }

    @Test
    public void shouldCountReferences() {
        final TinkerByteBuffer buf = new TinkerByteBuffer();
        assertEquals(1, buf.referenceCount());

        buf.retain();
        assertEquals(2, buf.referenceCount());
        assertFalse("still referenced, so release does not report the last one", buf.release());
        assertEquals(1, buf.referenceCount());
        assertTrue("the final release reports that the buffer is done", buf.release());
    }

    @Test
    public void shouldReportAsHeapBacked() {
        assertFalse(new TinkerByteBuffer().isDirect());
    }
}
