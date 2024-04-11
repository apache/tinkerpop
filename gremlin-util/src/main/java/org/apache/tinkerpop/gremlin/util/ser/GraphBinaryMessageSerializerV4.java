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
package org.apache.tinkerpop.gremlin.util.ser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryMapper;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.Marker;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatus;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.binary.RequestMessageSerializerV4;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

public class GraphBinaryMessageSerializerV4 extends AbstractGraphBinaryMessageSerializerV1
    implements MessageTextSerializerV4<GraphBinaryMapper> {

    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();
    private static final String MIME_TYPE = SerTokens.MIME_GRAPHBINARY_V4;
    private final byte[] header = MIME_TYPE.getBytes(UTF_8);
    private final RequestMessageSerializerV4 requestSerializerV4;

    public GraphBinaryMessageSerializerV4() {
        this(TypeSerializerRegistry.INSTANCE);
    }

    public GraphBinaryMessageSerializerV4(final TypeSerializerRegistry registry) {
        super(registry);
        requestSerializerV4 = new RequestMessageSerializerV4();
    }

    @Override
    protected String obtainMimeType() {
        return MIME_TYPE;
    }

    @Override
    protected String obtainStringdMimeType() {
        return ""; // stringd not currently supported.
    }

    @Override
    public ByteBuf serializeRequestMessageV4(RequestMessageV4 requestMessage, ByteBufAllocator allocator) throws SerializationException {
        final ByteBuf buffer = allocator.buffer().writeByte(header.length).writeBytes(header);

        try {
            requestSerializerV4.writeValue(requestMessage, buffer, writer);
        } catch (Exception ex) {
            buffer.release();
            throw ex;
        }

        return buffer;
    }

    @Override
    public RequestMessageV4 deserializeRequestMessageV4(ByteBuf msg) throws SerializationException {
        return requestSerializerV4.readValue(msg, reader);
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        return writeHeader(responseMessage, allocator);
    }

    @Override
    public String serializeResponseAsString(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        throw new UnsupportedOperationException("Response serialization as String is not supported");
    }

    //////////////// chunked write
    @Override
    public ByteBuf writeHeader(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        final EnumSet<MessageParts> parts = responseMessage.getStatus() != null ? MessageParts.ALL : MessageParts.START;

        return write(responseMessage, null, allocator, parts);
    }

    @Override
    public ByteBuf writeChunk(final Object aggregate, final ByteBufAllocator allocator) throws SerializationException {
        return write(null, aggregate, allocator, MessageParts.CHUNK);
    }

    @Override
    public ByteBuf writeFooter(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        return write(responseMessage, null, allocator, MessageParts.END);
    }

    @Override
    public ByteBuf writeErrorFooter(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        return write(responseMessage, null, allocator, MessageParts.ERROR);
    }

    private ByteBuf write(final ResponseMessage responseMessage, final Object aggregate,
                          final ByteBufAllocator allocator, final EnumSet<MessageParts> parts) throws SerializationException {
        final ByteBuf byteBuf = allocator.buffer();
        final Buffer buffer = bufferFactory.create(byteBuf);

        try {
            if (parts.contains(MessageParts.HEADER)) {
                // Version
                buffer.writeByte(GraphBinaryWriter.VERSION_BYTE);

                // Nullable request id
                writer.writeValue(responseMessage.getRequestId(), buffer, true);
                // Nullable tx id, todo: add real value when ready
                writer.writeValue((UUID)null, buffer, true);
            }

            if (parts.contains(MessageParts.DATA)) {
                final Object data = aggregate == null && responseMessage.getResult() != null
                        ? responseMessage.getResult().getData()
                        : aggregate;
                if (data != null) {
                    for (final Object item : (List) data) {
                        writer.write(item, buffer);
                    }
                }
            }

            if (parts.contains(MessageParts.FOOTER)) {
                final ResponseStatus status = responseMessage.getStatus();

                // we don't know how much data we have, so need a special object
                writer.write(Marker.END_OF_STREAM, buffer);
                // Status code
                writer.writeValue(status.getCode().getValue(), buffer, false);
                // Nullable status message
                writer.writeValue(status.getMessage(), buffer, true);
            }
        } catch (IOException e) {
            throw new SerializationException(e);
        }
        return byteBuf;
    }

    //////////////// read message methods

    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException {
        return readChunk(msg, true);
    }

    private List<Object> readPayload(final Buffer buffer) throws IOException {
        final List<Object> result = new ArrayList<>();
        while (buffer.readableBytes() != 0) {
            final Object obj = reader.read(buffer);
            if (Marker.END_OF_STREAM.equals(obj)) {
                break;
            }
            result.add(obj);
        }
        return result;
    }

    private Pair<ResponseStatusCode, String> readFooter(final Buffer buffer) throws IOException {
        return Pair.with(ResponseStatusCode.getFromValue(reader.readValue(buffer, Integer.class, false)),
                reader.readValue(buffer, String.class, true));
    }

    @Override
    public ResponseMessage readChunk(final ByteBuf byteBuf, final boolean isFirstChunk) throws SerializationException {
        final Buffer buffer = bufferFactory.create(byteBuf);

        try {
            // empty input buffer
            if (buffer.readableBytes() == 0) {
                return ResponseMessage.buildV4(null).
                        code(ResponseStatusCode.NO_CONTENT).result(Collections.emptyList()).create();
            }

            UUID requestId = null;

            if (isFirstChunk) {
                final int version = buffer.readByte() & 0xff;

                if (version >>> 7 != 1) {
                    // This is an indication that the response buffer was incorrectly built
                    // Or the buffer offsets are wrong
                    throw new SerializationException("The most significant bit should be set according to the format");
                }

                requestId = reader.readValue(buffer, UUID.class, true);
                // todo: handle tx id
                reader.readValue(buffer, UUID.class, true);
            }

            final List<Object> result = readPayload(buffer);

            // no footer
            if (buffer.readableBytes() == 0) {
                return ResponseMessage.buildV4(requestId)
                        .result(result)
                        .create();
            }

            final Pair<ResponseStatusCode, String> footer = readFooter(buffer);
            return ResponseMessage.buildV4(requestId)
                    .result(result)
                    .code(footer.getValue0())
                    .statusMessage(footer.getValue1())
                    .create();

        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }
}
