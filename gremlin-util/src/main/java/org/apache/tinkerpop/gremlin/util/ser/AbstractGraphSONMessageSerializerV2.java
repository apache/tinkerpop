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
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGraphSONMessageSerializerV2 extends AbstractMessageSerializer<ObjectMapper> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGraphSONMessageSerializerV2.class);

    protected ObjectMapper mapper;

    public AbstractGraphSONMessageSerializerV2() {
        final GraphSONMapper.Builder builder = configureBuilder(initBuilder(null));
        mapper = builder.create().createMapper();
    }

    @Deprecated
    public AbstractGraphSONMessageSerializerV2(final GraphSONMapper mapper) {
        this.mapper = mapper.createMapper();
    }

    public AbstractGraphSONMessageSerializerV2(final GraphSONMapper.Builder mapperBuilder) {
        this.mapper = configureBuilder(mapperBuilder).create().createMapper();
    }

    abstract byte[] obtainHeader();

    abstract GraphSONMapper.Builder configureBuilder(final GraphSONMapper.Builder builder);

    @Override
    public void configure(final Map<String, Object> config, final Map<String, Graph> graphs) {
        final GraphSONMapper.Builder initialBuilder = initBuilder(null);
        addIoRegistries(config, initialBuilder);
        applyMaxTokenLimits(initialBuilder, config);
        mapper = configureBuilder(initialBuilder).create().createMapper();
    }

    @Override
    public ByteBuf serializeResponseAsBinary(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException {
        ByteBuf encodedMessage = null;
        try {
            final byte[] payload = mapper.writeValueAsBytes(responseMessage);
            encodedMessage = allocator.buffer(payload.length);
            encodedMessage.writeBytes(payload);

            return encodedMessage;
        } catch (Exception ex) {
            if (encodedMessage != null) ReferenceCountUtil.release(encodedMessage);

            logger.warn(String.format("Response [%s] could not be serialized by %s.", responseMessage, AbstractGraphSONMessageSerializerV2.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public ByteBuf serializeRequestAsBinary(final RequestMessage requestMessage, final ByteBufAllocator allocator) throws SerializationException {
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
    public RequestMessage deserializeRequest(final ByteBuf msg) throws SerializationException {
        try {
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            return mapper.readValue(payload, RequestMessage.class);
        } catch (Exception ex) {
            logger.warn(String.format("Request [%s] could not be deserialized by %s.", msg, AbstractGraphSONMessageSerializerV2.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException {
        try {
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            return mapper.readValue(payload, ResponseMessage.class);
        } catch (Exception ex) {
            logger.warn(String.format("Response [%s] could not be deserialized by %s.", msg, AbstractGraphSONMessageSerializerV2.class.getName()), ex);
            throw new SerializationException(ex);
        }
    }

    private GraphSONMapper.Builder initBuilder(final GraphSONMapper.Builder builder) {
        final GraphSONMapper.Builder b = null == builder ? GraphSONMapper.build() : builder;
        return b.addCustomModule(GraphSONXModuleV2.build())
                .version(GraphSONVersion.V2_0);
    }

    private GraphSONMapper.Builder applyMaxTokenLimits(final GraphSONMapper.Builder builder, final Map<String, Object> config) {
        if(config != null) {
            if(config.containsKey("maxNumberLength")) {
                builder.maxNumberLength((int)config.get("maxNumberLength"));
            }
            if(config.containsKey("maxStringLength")) {
                builder.maxStringLength((int)config.get("maxStringLength"));
            }
            if(config.containsKey("maxNestingDepth")) {
                builder.maxNestingDepth((int)config.get("maxNestingDepth"));
            }
        }
        return builder;
    }

    @Override
    public ObjectMapper getMapper() {
        return this.mapper;
    }

    public final static class GremlinServerModule extends SimpleModule {
        public GremlinServerModule() {
            super("graphson-gremlin-server");

            // SERIALIZERS
            addSerializer(ResponseMessage.class, new ResponseMessageSerializer());
            addSerializer(RequestMessage.class, new RequestMessageSerializer());

            //DESERIALIZERS
            addDeserializer(ResponseMessage.class, new ResponseMessageDeserializer());
            addDeserializer(RequestMessage.class, new RequestMessageDeserializer());
        }
    }

    public final static class RequestMessageSerializer extends StdSerializer<RequestMessage> {
        public RequestMessageSerializer() {
            super(RequestMessage.class);
        }

        @Override
        public void serialize(final RequestMessage requestMessage, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            ser(requestMessage, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final RequestMessage requestMessage, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider,
                                      final TypeSerializer typeSerializer) throws IOException {
            ser(requestMessage, jsonGenerator, serializerProvider, typeSerializer);
        }

        public void ser(final RequestMessage requestMessage, final JsonGenerator jsonGenerator,
                        final SerializerProvider serializerProvider,
                        final TypeSerializer typeSerializer) throws IOException {
            GraphSONUtil.writeStartObject(requestMessage, jsonGenerator, typeSerializer);

            jsonGenerator.writeStringField(SerTokens.TOKEN_REQUEST, requestMessage.getRequestId().toString());
            jsonGenerator.writeStringField(SerTokens.TOKEN_OP, requestMessage.getOp());
            jsonGenerator.writeStringField(SerTokens.TOKEN_PROCESSOR, requestMessage.getProcessor());
            jsonGenerator.writeObjectField(SerTokens.TOKEN_ARGS, requestMessage.getArgs());

            GraphSONUtil.writeEndObject(requestMessage, jsonGenerator, typeSerializer);
        }
    }

    public final static class RequestMessageDeserializer extends AbstractObjectDeserializer<RequestMessage> {
        protected RequestMessageDeserializer() {
            super(RequestMessage.class);
        }

        @Override
        public RequestMessage createObject(final Map<String, Object> data) {
            final Map<String, Object> args = (Map<String, Object>) data.get(SerTokens.TOKEN_ARGS);
            RequestMessage.Builder builder = RequestMessage.build(data.get(SerTokens.TOKEN_OP).toString())
                    .overrideRequestId(UUID.fromString(data.get(SerTokens.TOKEN_REQUEST).toString()));

            if (data.containsKey(SerTokens.TOKEN_PROCESSOR))
                builder = builder.processor(data.get(SerTokens.TOKEN_PROCESSOR).toString());

            if (args != null) {
                for (Map.Entry<String, Object> kv : args.entrySet()) {
                    builder = builder.addArg(kv.getKey(), kv.getValue());
                }
            }

            return builder.create();
        }
    }

    public final static class ResponseMessageSerializer extends StdSerializer<ResponseMessage> {
        public ResponseMessageSerializer() {
            super(ResponseMessage.class);
        }

        @Override
        public void serialize(final ResponseMessage responseMessage, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            ser(responseMessage, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final ResponseMessage responseMessage, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider,
                                      final TypeSerializer typeSerializer) throws IOException {
            ser(responseMessage, jsonGenerator, serializerProvider, typeSerializer);
        }

        public void ser(final ResponseMessage responseMessage, final JsonGenerator jsonGenerator,
                        final SerializerProvider serializerProvider,
                        final TypeSerializer typeSerializer) throws IOException {
            GraphSONUtil.writeStartObject(responseMessage, jsonGenerator, typeSerializer);

            jsonGenerator.writeStringField(SerTokens.TOKEN_REQUEST, responseMessage.getRequestId() != null ? responseMessage.getRequestId().toString() : null);
            jsonGenerator.writeFieldName(SerTokens.TOKEN_STATUS);

            GraphSONUtil.writeStartObject(responseMessage, jsonGenerator, typeSerializer);
            jsonGenerator.writeStringField(SerTokens.TOKEN_MESSAGE, responseMessage.getStatus().getMessage());
            jsonGenerator.writeNumberField(SerTokens.TOKEN_CODE, responseMessage.getStatus().getCode().getValue());
            jsonGenerator.writeObjectField(SerTokens.TOKEN_ATTRIBUTES, responseMessage.getStatus().getAttributes());
            GraphSONUtil.writeEndObject(responseMessage, jsonGenerator, typeSerializer);

            jsonGenerator.writeFieldName(SerTokens.TOKEN_RESULT);

            GraphSONUtil.writeStartObject(responseMessage, jsonGenerator, typeSerializer);

            if (null == responseMessage.getResult().getData()){
                jsonGenerator.writeNullField(SerTokens.TOKEN_DATA);
            } else {
                jsonGenerator.writeFieldName(SerTokens.TOKEN_DATA);
                final Object result = responseMessage.getResult().getData();
                serializerProvider.findTypedValueSerializer(result.getClass(), true, null).serialize(result, jsonGenerator, serializerProvider);
            }

            jsonGenerator.writeObjectField(SerTokens.TOKEN_META, responseMessage.getResult().getMeta());
            GraphSONUtil.writeEndObject(responseMessage, jsonGenerator, typeSerializer);

            GraphSONUtil.writeEndObject(responseMessage, jsonGenerator, typeSerializer);
        }
    }
    
    public final static class ResponseMessageDeserializer extends AbstractObjectDeserializer<ResponseMessage> {
        protected ResponseMessageDeserializer() {
            super(ResponseMessage.class);
        }
        
        @Override
        public ResponseMessage createObject(final Map<String, Object> data) {
            final Map<String, Object> status = (Map<String, Object>) data.get(SerTokens.TOKEN_STATUS);
            final Map<String, Object> result = (Map<String, Object>) data.get(SerTokens.TOKEN_RESULT);
            return ResponseMessage.build(UUID.fromString(data.get(SerTokens.TOKEN_REQUEST).toString()))
                    .code(ResponseStatusCode.getFromValue((Integer) status.get(SerTokens.TOKEN_CODE)))
                    .statusMessage(String.valueOf(status.get(SerTokens.TOKEN_MESSAGE)))
                    .statusAttributes((Map<String, Object>) status.get(SerTokens.TOKEN_ATTRIBUTES))
                    .result(result.get(SerTokens.TOKEN_DATA))
                    .responseMetaData((Map<String, Object>) result.get(SerTokens.TOKEN_META))
                    .create();
        }
    }
}
