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

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.BufferFactory;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HeapBufferTest {

    private final HeapBufferFactory factory = new HeapBufferFactory();

    @Test
    public void shouldWriteAndReadByte() {
        final Buffer buffer = factory.create(16);
        buffer.writeByte(0x7F);
        assertEquals((byte) 0x7F, buffer.readByte());
    }

    @Test
    public void shouldWriteAndReadBoolean() {
        final Buffer buffer = factory.create(16);
        buffer.writeBoolean(true);
        buffer.writeBoolean(false);
        assertTrue(buffer.readBoolean());
        assertFalse(buffer.readBoolean());
    }

    @Test
    public void shouldWriteAndReadShort() {
        final Buffer buffer = factory.create(16);
        buffer.writeShort(12345);
        assertEquals((short) 12345, buffer.readShort());
    }

    @Test
    public void shouldWriteAndReadInt() {
        final Buffer buffer = factory.create(16);
        buffer.writeInt(Integer.MAX_VALUE);
        buffer.writeInt(Integer.MIN_VALUE);
        assertEquals(Integer.MAX_VALUE, buffer.readInt());
        assertEquals(Integer.MIN_VALUE, buffer.readInt());
    }

    @Test
    public void shouldWriteAndReadLong() {
        final Buffer buffer = factory.create(16);
        buffer.writeLong(Long.MAX_VALUE);
        buffer.writeLong(Long.MIN_VALUE);
        assertEquals(Long.MAX_VALUE, buffer.readLong());
        assertEquals(Long.MIN_VALUE, buffer.readLong());
    }

    @Test
    public void shouldWriteAndReadFloat() {
        final Buffer buffer = factory.create(16);
        buffer.writeFloat(3.14f);
        assertEquals(3.14f, buffer.readFloat(), 0.0f);
    }

    @Test
    public void shouldWriteAndReadDouble() {
        final Buffer buffer = factory.create(16);
        buffer.writeDouble(3.14159265358979);
        assertEquals(3.14159265358979, buffer.readDouble(), 0.0);
    }

    @Test
    public void shouldWriteAndReadBytes() {
        final Buffer buffer = factory.create(16);
        final byte[] src = {1, 2, 3, 4, 5};
        buffer.writeBytes(src);
        final byte[] dst = new byte[5];
        buffer.readBytes(dst);
        assertArrayEquals(src, dst);
    }

    @Test
    public void shouldWriteAndReadBytesWithOffset() {
        final Buffer buffer = factory.create(16);
        final byte[] src = {10, 20, 30, 40, 50};
        buffer.writeBytes(src, 1, 3);
        final byte[] dst = new byte[5];
        buffer.readBytes(dst, 2, 3);
        assertArrayEquals(new byte[]{0, 0, 20, 30, 40}, dst);
    }

    @Test
    public void shouldWriteAndReadByteBuffer() {
        final Buffer buffer = factory.create(16);
        final ByteBuffer src = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        buffer.writeBytes(src);
        final ByteBuffer dst = ByteBuffer.allocate(4);
        buffer.readBytes(dst);
        dst.flip();
        assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}), dst);
    }

    @Test
    public void shouldReadBytesToOutputStream() throws IOException {
        final Buffer buffer = factory.create(16);
        buffer.writeBytes(new byte[]{10, 20, 30});
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        buffer.readBytes(out, 3);
        assertArrayEquals(new byte[]{10, 20, 30}, out.toByteArray());
    }

    @Test
    public void shouldSupportGetBytes() {
        final Buffer buffer = factory.create(16);
        buffer.writeBytes(new byte[]{1, 2, 3, 4, 5});
        final byte[] dst = new byte[3];
        buffer.getBytes(1, dst);
        assertArrayEquals(new byte[]{2, 3, 4}, dst);
        // reader index should not have changed
        assertEquals(0, buffer.readerIndex());
    }

    @Test
    public void shouldAutoGrow() {
        final Buffer buffer = factory.create(4);
        buffer.writeInt(1);
        buffer.writeInt(2);
        buffer.writeInt(3);
        assertEquals(1, buffer.readInt());
        assertEquals(2, buffer.readInt());
        assertEquals(3, buffer.readInt());
    }

    @Test
    public void shouldTrackReaderAndWriterIndex() {
        final Buffer buffer = factory.create(16);
        assertEquals(0, buffer.readerIndex());
        assertEquals(0, buffer.writerIndex());
        buffer.writeInt(42);
        assertEquals(0, buffer.readerIndex());
        assertEquals(4, buffer.writerIndex());
        buffer.readInt();
        assertEquals(4, buffer.readerIndex());
        assertEquals(4, buffer.writerIndex());
        assertEquals(0, buffer.readableBytes());
    }

    @Test
    public void shouldSupportMarkAndResetWriterIndex() {
        final Buffer buffer = factory.create(16);
        buffer.writeInt(1);
        buffer.markWriterIndex();
        buffer.writeInt(2);
        assertEquals(8, buffer.writerIndex());
        buffer.resetWriterIndex();
        assertEquals(4, buffer.writerIndex());
    }

    @Test
    public void shouldReturnNioBuffer() {
        final Buffer buffer = factory.create(16);
        buffer.writeBytes(new byte[]{1, 2, 3});
        final ByteBuffer nio = buffer.nioBuffer();
        assertEquals(3, nio.remaining());
        assertEquals(1, nio.get());
        assertEquals(2, nio.get());
        assertEquals(3, nio.get());
    }

    @Test
    public void shouldReturnNioBufferWithIndexAndLength() {
        final Buffer buffer = factory.create(16);
        buffer.writeBytes(new byte[]{1, 2, 3, 4, 5});
        final ByteBuffer nio = buffer.nioBuffer(1, 3);
        assertEquals(3, nio.remaining());
        assertEquals(2, nio.get());
        assertEquals(3, nio.get());
        assertEquals(4, nio.get());
    }

    @Test
    public void shouldSupportReferenceCount() {
        final Buffer buffer = factory.create(16);
        assertEquals(1, buffer.referenceCount());
        buffer.retain();
        assertEquals(2, buffer.referenceCount());
        assertFalse(buffer.release());
        assertEquals(1, buffer.referenceCount());
        assertTrue(buffer.release());
        assertEquals(0, buffer.referenceCount());
    }

    @Test
    public void shouldNotBeDirect() {
        final Buffer buffer = factory.create(16);
        assertFalse(buffer.isDirect());
    }

    @Test
    public void shouldReturnNioBufferCount() {
        final Buffer buffer = factory.create(16);
        assertEquals(1, buffer.nioBufferCount());
    }

    @Test
    public void shouldWrapByteBuffer() {
        final ByteBuffer src = ByteBuffer.wrap(new byte[]{5, 6, 7, 8});
        final Buffer buffer = factory.wrap(src);
        assertEquals(4, buffer.readableBytes());
        assertEquals(5, buffer.readByte());
        assertEquals(6, buffer.readByte());
    }

    @Test
    public void shouldCreateFromByteArray() {
        final Buffer buffer = factory.create(new byte[]{1, 2, 3});
        assertEquals(3, buffer.readableBytes());
        assertEquals(1, buffer.readByte());
        assertEquals(2, buffer.readByte());
        assertEquals(3, buffer.readByte());
    }

    @Test
    public void shouldRoundTripRequestMessageWithGraphBinarySerializer() throws SerializationException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        final HeapBufferFactory bufferFactory = new HeapBufferFactory();

        final RequestMessage request = RequestMessage.build("g.V().count()")
                .addLanguage("gremlin-groovy")
                .addG("g")
                .create();

        // Serialize using the serializer (which uses NettyBufferFactory internally)
        final Buffer serialized = serializer.serializeRequestAsBinary(request);

        // Copy bytes into a HeapBuffer to prove HeapBuffer can deserialize
        final byte[] bytes = new byte[serialized.readableBytes()];
        serialized.readBytes(bytes);
        serialized.release();

        final Buffer heapBuf = bufferFactory.create(bytes);
        final RequestMessage deserialized = serializer.deserializeBinaryRequest(heapBuf);

        assertEquals(request.getGremlin(), deserialized.getGremlin());
        assertEquals(request.getFields().get("language"), deserialized.getFields().get("language"));
        assertEquals(request.getFields().get("g"), deserialized.getFields().get("g"));
    }

    @Test
    public void shouldRoundTripResponseMessageWithGraphBinarySerializer() throws SerializationException {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        final HeapBufferFactory bufferFactory = new HeapBufferFactory();

        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .result(Arrays.asList(1, "test", 3.14))
                .create();

        // Serialize
        final Buffer serialized = serializer.serializeResponseAsBinary(response);

        // Copy to HeapBuffer for deserialization
        final byte[] bytes = new byte[serialized.readableBytes()];
        serialized.readBytes(bytes);
        serialized.release();

        final Buffer heapBuf = bufferFactory.create(bytes);
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(heapBuf);

        assertEquals(response.getStatus().getCode(), deserialized.getStatus().getCode());
        assertEquals(response.getResult().getData(), deserialized.getResult().getData());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldThrowOnReadPastWriterIndex() {
        final Buffer buffer = factory.create(16);
        buffer.writeByte(1);
        buffer.readByte();
        buffer.readByte(); // should throw
    }

    @Test
    public void shouldSetReaderIndex() {
        final Buffer buffer = factory.create(16);
        buffer.writeInt(42);
        buffer.readInt();
        buffer.readerIndex(0);
        assertEquals(42, buffer.readInt());
    }

    @Test
    public void shouldSetWriterIndex() {
        final Buffer buffer = factory.create(16);
        buffer.writeInt(42);
        buffer.writerIndex(0);
        assertEquals(0, buffer.writerIndex());
        assertEquals(0, buffer.readableBytes());
    }
}
