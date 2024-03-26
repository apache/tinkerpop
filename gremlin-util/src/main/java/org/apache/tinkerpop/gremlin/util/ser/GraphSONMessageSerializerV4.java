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
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Serialize results to JSON with version 4.0.x schema and the extended module.
 */
public final class GraphSONMessageSerializerV4 extends AbstractGraphSONMessageSerializerV2
        implements MessageTextSerializerV4<ObjectMapper>, MessageChunkSerializer<ObjectMapper> {

    public final static class GremlinServerModuleV4 extends SimpleModule {
        public GremlinServerModuleV4() {
            super("graphsonV4-gremlin-server");

            // SERIALIZERS
            addSerializer(ResponseMessage.class, new ResponseMessageSerializer());
            addSerializer(ResponseMessage.ResponseMessageHeader.class, new ResponseMessageHeaderSerializer());
            addSerializer(ResponseMessage.ResponseMessageFooter.class, new ResponseMessageFooterSerializer());
            addSerializer(RequestMessageV4.class, new GraphSONMessageSerializerV4.RequestMessageV4Serializer());

            // DESERIALIZERS
            addDeserializer(ResponseMessage.class, new ResponseMessageDeserializer());
            addDeserializer(RequestMessageV4.class, new GraphSONMessageSerializerV4.RequestMessageV4Deserializer());
        }
    }

    public final static class RequestMessageV4Serializer extends StdSerializer<RequestMessageV4> {
        public RequestMessageV4Serializer() {
            super(RequestMessageV4.class);
        }

        @Override
        public void serialize(final RequestMessageV4 requestMessage, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            ser(requestMessage, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final RequestMessageV4 requestMessage, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider,
                                      final TypeSerializer typeSerializer) throws IOException {
            ser(requestMessage, jsonGenerator, serializerProvider, typeSerializer);
        }

        public void ser(final RequestMessageV4 requestMessage, final JsonGenerator jsonGenerator,
                        final SerializerProvider serializerProvider,
                        final TypeSerializer typeSerializer) throws IOException {
            GraphSONUtil.writeStartObject(requestMessage, jsonGenerator, typeSerializer);

            jsonGenerator.writeObjectField(SerTokens.TOKEN_GREMLIN, requestMessage.getGremlin());
            for (Map.Entry<String, Object> kv : requestMessage.getFields().entrySet()) {
                jsonGenerator.writeObjectField(kv.getKey(), kv.getValue());
            }

            GraphSONUtil.writeEndObject(requestMessage, jsonGenerator, typeSerializer);
        }
    }

    public final static class RequestMessageV4Deserializer extends AbstractObjectDeserializer<RequestMessageV4> {
        protected RequestMessageV4Deserializer() {
            super(RequestMessageV4.class);
        }

        @Override
        public RequestMessageV4 createObject(final Map<String, Object> data) {
            RequestMessageV4.Builder builder = RequestMessageV4.build(data.get(SerTokens.TOKEN_GREMLIN));

            if (data.containsKey(SerTokens.TOKEN_REQUEST)) {
                builder.overrideRequestId(UUID.fromString(data.get(SerTokens.TOKEN_REQUEST).toString()));
            }
            if (data.containsKey(SerTokens.TOKEN_LANGUAGE)) {
                builder.addLanguage(data.get(SerTokens.TOKEN_LANGUAGE).toString());
            }
            if (data.containsKey(SerTokens.TOKEN_G)) {
                builder.addG(data.get(SerTokens.TOKEN_G).toString());
            }
            if (data.containsKey(SerTokens.TOKEN_BINDINGS)) {
                builder.addBindings((Map<String, Object>) data.get(SerTokens.TOKEN_BINDINGS));
            }

            return builder.create();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(GraphSONMessageSerializerV3.class);
    private static final String MIME_TYPE = SerTokens.MIME_GRAPHSON_V4;

    private static byte[] header;

    static {
        final ByteBuffer buffer = ByteBuffer.allocate(MIME_TYPE.length() + 1);
        buffer.put((byte) MIME_TYPE.length());
        buffer.put(MIME_TYPE.getBytes());
        header = buffer.array();
    }

    /**
     * Creates a default GraphSONMessageSerializer.
     * <p>
     * By default this will internally instantiate a {@link GraphSONMapper} and register
     * a {@link GremlinServerModule} and {@link GraphSONXModuleV3} to the mapper.
     *
     * @see #GraphSONMessageSerializerV4(GraphSONMapper.Builder)
     */
    public GraphSONMessageSerializerV4() {
        super();
    }

    /**
     * Create a GraphSONMessageSerializer from a {@link GraphSONMapper}. Deprecated, use
     * {@link #GraphSONMessageSerializerV4(GraphSONMapper.Builder)} instead.
     */
    @Deprecated
    public GraphSONMessageSerializerV4(final GraphSONMapper mapper) {
        super(mapper);
    }

    /**
     * Create a GraphSONMessageSerializer with a provided {@link GraphSONMapper.Builder}.
     *
     * Note that to make this mapper usable in the context of request messages and responses,
     * this method will automatically register a {@link GremlinServerModule} to the provided
     * mapper.
     */
    public GraphSONMessageSerializerV4(final GraphSONMapper.Builder mapperBuilder) {
        super(mapperBuilder);
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{MIME_TYPE, SerTokens.MIME_JSON};
    }

    @Override
    GraphSONMapper.Builder configureBuilder(final GraphSONMapper.Builder builder) {
        // override the 2.0 in AbstractGraphSONMessageSerializerV2
        return builder.version(GraphSONVersion.V4_0).addCustomModule(new GremlinServerModuleV4());
    }

    @Override
    byte[] obtainHeader() {
        return header;
    }

    @Override
    public ResponseMessage deserializeResponse(final String msg) throws SerializationException {
        try {
            return mapper.readValue(msg, ResponseMessage.class);
        } catch (Exception ex) {
            logger.warn(String.format("Response [%s] could not be deserialized by %s.", msg, GraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeResponseAsString(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        try {
            return mapper.writeValueAsString(responseMessage);
        } catch (Exception ex) {
            logger.warn(String.format("Response [%s] could not be serialized by %s.", responseMessage.toString(), GraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final String msg) throws SerializationException {
        try {
            return mapper.readValue(msg, RequestMessage.class);
        } catch (Exception ex) {
            logger.warn(String.format("Request [%s] could not be deserialized by %s.", msg, GraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeRequestAsString(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        try {
            return mapper.writeValueAsString(requestMessage);
        } catch (Exception ex) {
            logger.warn(String.format("Request [%s] could not be serialized by %s.", requestMessage.toString(), GraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    // !!!
    @Override
    public ByteBuf writeHeader(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final byte[] header = mapper.writeValueAsBytes(new ResponseMessage.ResponseMessageHeader(responseMessage));
            final byte[] result = getChunk(true, responseMessage.getResult().getData());
            // skip closing }
            encodedMessage = allocator.buffer(header.length - 1 + result.length);
            encodedMessage.writeBytes(header, 0, header.length - 1);
            encodedMessage.writeBytes(result);

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn(String.format("Response [%s] could not be serialized by %s.", responseMessage, AbstractGraphSONMessageSerializerV2.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    private byte[] getChunk(final boolean isFirst, final Object aggregate) throws JsonProcessingException {
        // todo: cleanup
        List asList = (List)aggregate;
        Object[] array = new Object[asList.size()];
        asList.toArray(array);
        String str = mapper.writeValueAsString(array);
        str = str.substring(1, str.length() - 1);
        if (!isFirst) {
            str = "," + str;
        }
        return str.getBytes();
    }

    @Override
    public ByteBuf writeChunk(final Object aggregate, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final byte[] payload = getChunk(false, aggregate);
            encodedMessage = allocator.buffer(payload.length);
            encodedMessage.writeBytes(payload);

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn(String.format("Response [%s] could not be serialized by %s.", aggregate, GraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf writeFooter(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final byte[] footer = mapper.writeValueAsBytes(new ResponseMessage.ResponseMessageFooter(responseMessage));
            final byte[] result = getChunk(false, responseMessage.getResult().getData());
            // skip opening {
            encodedMessage = allocator.buffer(footer.length - 1 + result.length);
            encodedMessage.writeBytes(result);
            encodedMessage.writeBytes(footer, 1, footer.length - 1);

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn(String.format("Response [%s] could not be serialized by %s.", responseMessage, GraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf writeErrorFooter(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final byte[] footer = mapper.writeValueAsBytes(new ResponseMessage.ResponseMessageFooter(responseMessage));
            // skip opening {
            encodedMessage = allocator.buffer(footer.length - 1);
            encodedMessage.writeBytes(footer, 1, footer.length - 1);

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn(String.format("Response [%s] could not be serialized by %s.", responseMessage, GraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public ResponseMessage readChunk(final ByteBuf byteBuf, final boolean isFirstChunk) {
        throw new IllegalStateException("Reading for streaming GraphSON is not supported");
    }

    @Override
    public ByteBuf serializeRequestMessageV4(RequestMessageV4 requestMessage, ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final byte[] header = obtainHeader();
            final byte[] payload = mapper.writeValueAsBytes(requestMessage);

            encodedMessage = allocator.buffer(header.length + payload.length);
            encodedMessage.writeBytes(header);
            encodedMessage.writeBytes(payload);

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn(String.format("Request [%s] could not be serialized by %s.", requestMessage, AbstractGraphSONMessageSerializerV2.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public RequestMessageV4 deserializeRequestMessageV4(ByteBuf msg) throws SerializationException {
        try {
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            return mapper.readValue(payload, RequestMessageV4.class);
        } catch (Exception ex) {
            logger.warn(String.format("Request [%s] could not be deserialized by %s.", msg, AbstractGraphSONMessageSerializerV2.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    public final static class ResponseMessageHeaderSerializer extends StdSerializer<ResponseMessage.ResponseMessageHeader> {
        public ResponseMessageHeaderSerializer() {
            super(ResponseMessage.ResponseMessageHeader.class);
        }

        @Override
        public void serialize(final ResponseMessage.ResponseMessageHeader responseMessage, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            ser(responseMessage, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final ResponseMessage.ResponseMessageHeader responseMessage, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider,
                                      final TypeSerializer typeSerializer) throws IOException {
            ser(responseMessage, jsonGenerator, serializerProvider, typeSerializer);
        }

        public void ser(final ResponseMessage.ResponseMessageHeader responseMessageHeader, final JsonGenerator jsonGenerator,
                        final SerializerProvider serializerProvider,
                        final TypeSerializer typeSerializer) throws IOException {
            final ResponseMessage responseMessage = responseMessageHeader.getResponseMessage();

            GraphSONUtil.writeStartObject(responseMessage, jsonGenerator, typeSerializer);

            jsonGenerator.writeStringField(SerTokens.TOKEN_REQUEST, responseMessage.getRequestId() != null ? responseMessage.getRequestId().toString() : null);
            // todo: write tx id

            jsonGenerator.writeFieldName(SerTokens.TOKEN_RESULT);

            jsonGenerator.writeRaw(":{\"@type\":\"g:List\",\"@value\":[");

            // jsonGenerator will add 2 closing }
            // jsonGenerator.configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);
        }
    }

    public final static class ResponseMessageFooterSerializer extends StdSerializer<ResponseMessage.ResponseMessageFooter> {
        public ResponseMessageFooterSerializer() {
            super(ResponseMessage.ResponseMessageFooter.class);
        }

        @Override
        public void serialize(final ResponseMessage.ResponseMessageFooter responseMessage, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            ser(responseMessage, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final ResponseMessage.ResponseMessageFooter responseMessage, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider,
                                      final TypeSerializer typeSerializer) throws IOException {
            ser(responseMessage, jsonGenerator, serializerProvider, typeSerializer);
        }

        public void ser(final ResponseMessage.ResponseMessageFooter responseMessageFooter, final JsonGenerator jsonGenerator,
                        final SerializerProvider serializerProvider,
                        final TypeSerializer typeSerializer) throws IOException {
            final ResponseMessage responseMessage = responseMessageFooter.getResponseMessage();

            // todo: find a way to get rid off
            GraphSONUtil.writeStartObject(responseMessage, jsonGenerator, typeSerializer);

            // close result field
            jsonGenerator.writeRaw("]},");

            jsonGenerator.writeFieldName(SerTokens.TOKEN_STATUS);
            GraphSONUtil.writeStartObject(responseMessage, jsonGenerator, typeSerializer);
            jsonGenerator.writeStringField(SerTokens.TOKEN_MESSAGE, responseMessage.getStatus().getMessage());
            jsonGenerator.writeNumberField(SerTokens.TOKEN_CODE, responseMessage.getStatus().getCode().getValue());
            GraphSONUtil.writeEndObject(responseMessage, jsonGenerator, typeSerializer);

            GraphSONUtil.writeEndObject(responseMessage, jsonGenerator, typeSerializer);
        }
    }
}
