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
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public abstract class AbstractGraphSONMessageSerializerV4 extends AbstractGraphSONMessageSerializerV2 implements MessageTextSerializerV4<ObjectMapper> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGraphSONMessageSerializerV4.class);

    public AbstractGraphSONMessageSerializerV4() {
        super();
    }

    public AbstractGraphSONMessageSerializerV4(GraphSONMapper.Builder mapperBuilder) {
        super(mapperBuilder);
    }

    @Override
    public abstract String[] mimeTypesSupported();

    @Override
    abstract byte[] obtainHeader();

    @Override
    public ResponseMessage deserializeResponse(final String msg) throws SerializationException {
        try {
            return mapper.readValue(msg, ResponseMessage.class);
        } catch (Exception ex) {
            logger.warn(String.format("Response [%s] could not be deserialized by %s.", msg, AbstractGraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public RequestMessage deserializeRequest(final String msg) throws SerializationException {
        try {
            return mapper.readValue(msg, RequestMessage.class);
        } catch (Exception ex) {
            logger.warn(String.format("Request [%s] could not be deserialized by %s.", msg, AbstractGraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public String serializeRequestAsString(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
        try {
            return mapper.writeValueAsString(requestMessage);
        } catch (Exception ex) {
            logger.warn(String.format("Request [%s] could not be serialized by %s.", requestMessage.toString(), AbstractGraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        return writeHeader(responseMessage, allocator);
    }

    @Override
    public String serializeResponseAsString(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        throw new UnsupportedOperationException("Response serialization as String is not supported");
    }

    protected boolean isTyped() { return true; };
    @Override
    public ByteBuf writeHeader(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            boolean writeFullMessage = responseMessage.getStatus() != null;

            final byte[] header = mapper.writeValueAsBytes(new ResponseMessage.ResponseMessageHeader(responseMessage, isTyped()));
            final byte[] data = getChunk(true, responseMessage.getResult().getData());

            final byte[] footer = writeFullMessage
                    ? mapper.writeValueAsBytes(new ResponseMessage.ResponseMessageFooter(responseMessage, isTyped()))
                    : new byte[0];
            // skip closing }
            final int headerLen = header.length - (isTyped() ? 3 : 2);
            final int bufSize = headerLen + data.length + (writeFullMessage ? footer.length - 1 : 0);

            encodedMessage = allocator.buffer(bufSize).writeBytes(header, 0, headerLen).writeBytes(data);

            if (writeFullMessage) {
                encodedMessage.writeBytes(footer, 1, footer.length - 1);
            }

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn(String.format("Response [%s] could not be serialized by %s.", responseMessage, AbstractGraphSONMessageSerializerV2.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    private byte[] getChunk(final boolean isFirst, final Object aggregate) throws JsonProcessingException {
        if (aggregate == null) {
            return new byte[0];
        }

        // Gremlin server always produce List
        final List asList = (List) aggregate;
        if (asList.isEmpty()) {
            return new byte[0];
        }

        final Object[] array = new Object[asList.size()];
        asList.toArray(array);
        // List serialization adds extra data, so array used
        String str = mapper.writeValueAsString(array);
        // skip opening `[` and closing `]`
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
            encodedMessage = allocator.buffer(payload.length).writeBytes(payload);

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
            final byte[] footer = mapper.writeValueAsBytes(new ResponseMessage.ResponseMessageFooter(responseMessage, isTyped()));
            final byte[] data = getChunk(false, responseMessage.getResult().getData());
            // skip opening {
            encodedMessage = allocator.buffer(footer.length - 2 + data.length).
                    writeBytes(data).writeBytes(footer, 1, footer.length - 1);

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
            final byte[] footer = mapper.writeValueAsBytes(new ResponseMessage.ResponseMessageFooter(responseMessage, isTyped()));
            // skip opening {
            encodedMessage = allocator.buffer(footer.length - 2).
                    writeBytes(footer, 1, footer.length - 1);

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn(String.format("Response [%s] could not be serialized by %s.", responseMessage, GraphSONMessageSerializerV4.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public ResponseMessage readChunk(final ByteBuf byteBuf, final boolean isFirstChunk) {
        throw new UnsupportedOperationException("Reading for streaming GraphSON is not supported");
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

            jsonGenerator.writeFieldName(SerTokens.TOKEN_RESULT);
            jsonGenerator.writeObject(Collections.emptyList());

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

            // close result field. array inside object for types, just array for untyped
            jsonGenerator.writeRaw(responseMessageFooter.getTyped() ? "]}," : "],");

            jsonGenerator.writeFieldName(SerTokens.TOKEN_STATUS);
            GraphSONUtil.writeStartObject(responseMessage, jsonGenerator, typeSerializer);
            jsonGenerator.writeStringField(SerTokens.TOKEN_MESSAGE, responseMessage.getStatus().getMessage());
            jsonGenerator.writeNumberField(SerTokens.TOKEN_CODE, responseMessage.getStatus().getCode().getValue());
            if (responseMessage.getStatus().getCode() != ResponseStatusCode.SUCCESS &&
                    responseMessage.getStatus().getException() != null) {
                jsonGenerator.writeStringField(SerTokens.TOKEN_EXCEPTION, responseMessage.getStatus().getException());
            }
            GraphSONUtil.writeEndObject(responseMessage, jsonGenerator, typeSerializer);

            GraphSONUtil.writeEndObject(responseMessage, jsonGenerator, typeSerializer);
        }
    }

    public final static class ResponseMessageV4Deserializer extends AbstractObjectDeserializer<ResponseMessage> {
        public ResponseMessageV4Deserializer() {
            super(ResponseMessage.class);
        }

        @Override
        public ResponseMessage createObject(final Map<String, Object> data) {
            final Map<String, Object> status = (Map<String, Object>) data.get(SerTokens.TOKEN_STATUS);
            return ResponseMessage.build()
                    .code(ResponseStatusCode.getFromValue((Integer) status.get(SerTokens.TOKEN_CODE)))
                    .statusMessage(String.valueOf(status.get(SerTokens.TOKEN_MESSAGE)))
                    .exception(String.valueOf(status.get(SerTokens.TOKEN_EXCEPTION)))
                    .result(data.get(SerTokens.TOKEN_RESULT))
                    .create();
        }
    }
}
