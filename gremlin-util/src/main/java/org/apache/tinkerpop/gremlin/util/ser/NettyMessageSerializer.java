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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;

/**
 * Adapter that wraps a {@link MessageSerializer} (which uses {@link Buffer}) and exposes methods with
 * Netty's {@link ByteBuf}/{@link ByteBufAllocator} signatures for use in gremlin-server's Netty pipeline.
 */
public class NettyMessageSerializer {

    private final MessageSerializer<?> serializer;
    private final NettyBufferFactory bufferFactory;

    public NettyMessageSerializer(final MessageSerializer<?> serializer) {
        this.serializer = serializer;
        this.bufferFactory = new NettyBufferFactory();
    }

    public MessageSerializer<?> getSerializer() {
        return serializer;
    }

    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        // Note: the allocator parameter is currently ignored because the underlying MessageSerializer
        // uses BufferFactory.create(int) which defaults to ByteBufAllocator.DEFAULT internally.
        // This will become relevant when BufferFactory is made injectable/configurable.
        final Buffer buffer = serializer.serializeResponseAsBinary(responseMessage);
        return extractByteBuf(buffer);
    }

    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        // Note: the allocator parameter is currently ignored — see serializeResponseAsBinary for details.
        final Buffer buffer = serializer.serializeRequestAsBinary(requestMessage);
        return extractByteBuf(buffer);
    }

    public RequestMessage deserializeBinaryRequest(final ByteBuf msg) throws SerializationException {
        final Buffer buffer = bufferFactory.create(msg);
        return serializer.deserializeBinaryRequest(buffer);
    }

    public ResponseMessage deserializeBinaryResponse(final ByteBuf msg) throws SerializationException {
        final Buffer buffer = bufferFactory.create(msg);
        return serializer.deserializeBinaryResponse(buffer);
    }

    public ByteBuf writeHeader(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        final Buffer buffer = serializer.writeHeader(responseMessage);
        return extractByteBuf(buffer);
    }

    public ByteBuf writeChunk(final Object aggregate, final ByteBufAllocator allocator) throws SerializationException {
        final Buffer buffer = serializer.writeChunk(aggregate);
        return extractByteBuf(buffer);
    }

    public ByteBuf writeFooter(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        final Buffer buffer = serializer.writeFooter(responseMessage);
        return extractByteBuf(buffer);
    }

    public ByteBuf writeErrorFooter(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        final Buffer buffer = serializer.writeErrorFooter(responseMessage);
        return extractByteBuf(buffer);
    }

    public ResponseMessage readChunk(final ByteBuf byteBuf, final boolean isFirstChunk) throws SerializationException {
        final Buffer buffer = bufferFactory.create(byteBuf);
        return serializer.readChunk(buffer, isFirstChunk);
    }

    private ByteBuf extractByteBuf(final Buffer buffer) {
        if (buffer instanceof NettyBuffer) {
            return ((NettyBuffer) buffer).getUnderlyingBuffer();
        }
        // Fallback: copy bytes into a new ByteBuf
        final byte[] bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
        buffer.release();
        return io.netty.buffer.Unpooled.wrappedBuffer(bytes);
    }
}
