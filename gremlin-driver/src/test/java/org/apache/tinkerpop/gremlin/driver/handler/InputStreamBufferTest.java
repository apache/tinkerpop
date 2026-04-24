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
import org.junit.Test;

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
}
