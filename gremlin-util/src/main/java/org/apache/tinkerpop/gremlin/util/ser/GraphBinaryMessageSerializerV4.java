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
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryIo;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryMapper;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.Marker;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.CustomTypeSerializer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatus;
import org.apache.tinkerpop.gremlin.util.ser.binary.RequestMessageSerializer;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class GraphBinaryMessageSerializerV4 extends AbstractMessageSerializer<GraphBinaryMapper> {
    public static final String TOKEN_CUSTOM = "custom";
    public static final String TOKEN_BUILDER = "builder";

    private GraphBinaryReader reader;
    private GraphBinaryWriter writer;
    private RequestMessageSerializer requestSerializer;
    private final GraphBinaryMapper mapper;

    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();
    private static final String MIME_TYPE = SerTokens.MIME_GRAPHBINARY_V4;

    /**
     * Creates a new instance of the message serializer using the default type serializers.
     */
    public GraphBinaryMessageSerializerV4() {
        this(TypeSerializerRegistry.INSTANCE);
    }

    public GraphBinaryMessageSerializerV4(final TypeSerializerRegistry registry) {
        reader = new GraphBinaryReader(registry);
        writer = new GraphBinaryWriter(registry);
        mapper = new GraphBinaryMapper(writer, reader);

        requestSerializer = new RequestMessageSerializer();
    }

    public GraphBinaryMessageSerializerV4(final TypeSerializerRegistry.Builder builder) {
        this(builder.create());
    }

    @Override
    public GraphBinaryMapper getMapper() {
        return mapper;
    }

    @Override
    public void configure(final Map<String, Object> config, final Map<String, Graph> graphs) {
        final String builderClassName = (String) config.get(TOKEN_BUILDER);
        final TypeSerializerRegistry.Builder builder;

        if (builderClassName != null) {
            try {
                final Class<?> clazz = Class.forName(builderClassName);
                final Constructor<?> ctor = clazz.getConstructor();
                builder = (TypeSerializerRegistry.Builder) ctor.newInstance();
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        } else {
            builder = TypeSerializerRegistry.build();
        }

        final List<String> classNameList = getListStringFromConfig(TOKEN_IO_REGISTRIES, config);
        classNameList.forEach(className -> {
            try {
                final Class<?> clazz = Class.forName(className);
                try {
                    final Method instanceMethod = tryInstanceMethod(clazz);
                    final IoRegistry ioreg = (IoRegistry) instanceMethod.invoke(null);
                    final List<Pair<Class, CustomTypeSerializer>> classSerializers = ioreg.find(GraphBinaryIo.class, CustomTypeSerializer.class);
                    for (Pair<Class,CustomTypeSerializer> cs : classSerializers) {
                        builder.addCustomType(cs.getValue0(), cs.getValue1());
                    }
                } catch (Exception methodex) {
                    throw new IllegalStateException(String.format("Could not instantiate IoRegistry from an instance() method on %s", className), methodex);
                }
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        });

        addCustomClasses(config, builder);

        final TypeSerializerRegistry registry = builder.create();
        reader = new GraphBinaryReader(registry);
        writer = new GraphBinaryWriter(registry);

        requestSerializer = new RequestMessageSerializer();
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[] {MIME_TYPE};
    }

    private void addCustomClasses(final Map<String, Object> config, final TypeSerializerRegistry.Builder builder) {
        final List<String> classNameList = getListStringFromConfig(TOKEN_CUSTOM, config);

        classNameList.forEach(serializerDefinition -> {
            final String className;
            final String serializerName;
            if (serializerDefinition.contains(";")) {
                final String[] split = serializerDefinition.split(";");
                if (split.length != 2)
                    throw new IllegalStateException(String.format("Invalid format for serializer definition [%s] - expected <class>;<serializer-class>", serializerDefinition));

                className = split[0];
                serializerName = split[1];
            } else {
                throw new IllegalStateException(String.format("Invalid format for serializer definition [%s] - expected <class>;<serializer-class>", serializerDefinition));
            }

            try {
                final Class clazz = Class.forName(className);
                final Class serializerClazz = Class.forName(serializerName);
                final CustomTypeSerializer serializer = (CustomTypeSerializer) serializerClazz.newInstance();
                builder.addCustomType(clazz, serializer);
            } catch (Exception ex) {
                throw new IllegalStateException("CustomTypeSerializer could not be instantiated", ex);
            }
        });
    }

    @Override
    public ByteBuf serializeRequestAsBinary(RequestMessage requestMessage, ByteBufAllocator allocator) throws SerializationException {
        final ByteBuf buffer = allocator.buffer();

        try {
            requestSerializer.writeValue(requestMessage, buffer, writer);
        } catch (Exception ex) {
            buffer.release();
            throw ex;
        }

        return buffer;
    }

    @Override
    public RequestMessage deserializeBinaryRequest(ByteBuf msg) throws SerializationException {
        return requestSerializer.readValue(msg, reader);
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        if (null == responseMessage.getStatus()) {
            throw new SerializationException("ResponseStatus can't be null when serializing a full ResponseMessage.");
        }

        return writeHeader(responseMessage, allocator);
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
                if (responseMessage.getResult().isBulked()) {
                    buffer.writeByte(GraphBinaryWriter.BULKED_BYTE);
                } else {
                    buffer.writeByte((byte) 0);
                }
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
                writer.writeValue(status.getCode().code(), buffer, false);
                // Nullable status message
                writer.writeValue(status.getMessage(), buffer, true);
                // Nullable exception
                writer.writeValue(status.getException(), buffer, true);
            }
        } catch (IOException e) {
            throw new SerializationException(e);
        }
        return byteBuf;
    }

    //////////////// read message methods

    @Override
    public ResponseMessage deserializeBinaryResponse(final ByteBuf msg) throws SerializationException {
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

    private Triplet<HttpResponseStatus, String, String> readFooter(final Buffer buffer) throws IOException {
        final HttpResponseStatus statusCode = HttpResponseStatus.valueOf(reader.readValue(buffer, Integer.class, false));
        final String message = reader.readValue(buffer, String.class, true);
        final String exception = reader.readValue(buffer, String.class, true);

        return Triplet.with(statusCode, message, exception);
    }

    @Override
    public ResponseMessage readChunk(final ByteBuf byteBuf, final boolean isFirstChunk) throws SerializationException {
        final Buffer buffer = bufferFactory.create(byteBuf);
        boolean bulking = false;

        try {
            // empty input buffer
            if (buffer.readableBytes() == 0) {
                return ResponseMessage.build().result(Collections.emptyList()).create();
            }

            if (isFirstChunk) {
                final int version = buffer.readByte() & 0xff;

                if (version >>> 7 != 1) {
                    // This is an indication that the response buffer was incorrectly built
                    // Or the buffer offsets are wrong
                    throw new SerializationException("The most significant bit should be set according to the format");
                }
                bulking = (buffer.readByte() & 1) == 1;
            }

            final List<Object> result = readPayload(buffer);

            // no footer
            if (buffer.readableBytes() == 0) {
                return ResponseMessage.build()
                        .result(result)
                        .bulked(bulking)
                        .create();
            }

            final Triplet<HttpResponseStatus, String, String> footer = readFooter(buffer);
            return ResponseMessage.build()
                    .result(result)
                    .bulked(bulking)
                    .code(footer.getValue0())
                    .statusMessage(footer.getValue1())
                    .exception(footer.getValue2())
                    .create();

        } catch (IOException | IndexOutOfBoundsException ex) {
            throw new SerializationException(ex);
        }
    }
}
