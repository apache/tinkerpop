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
package org.apache.tinkerpop.gremlin.driver.ser;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import groovy.json.JsonBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGraphSONMessageSerializerV1d0 extends AbstractMessageSerializer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGraphSONMessageSerializerV1d0.class);

    protected ObjectMapper mapper;

    protected static final String TOKEN_USE_MAPPER_FROM_GRAPH = "useMapperFromGraph";

    protected final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<Map<String, Object>>() {
    };

    public AbstractGraphSONMessageSerializerV1d0() {
        final GraphSONMapper.Builder builder = configureBuilder(GraphSONMapper.build());
        mapper = builder.create().createMapper();
    }

    public AbstractGraphSONMessageSerializerV1d0(final GraphSONMapper mapper) {
        this.mapper = mapper.createMapper();
    }

    abstract byte[] obtainHeader();

    abstract GraphSONMapper.Builder configureBuilder(final GraphSONMapper.Builder builder);

    @Override
    public void configure(final Map<String, Object> config, final Map<String, Graph> graphs) {
        final GraphSONMapper.Builder initialBuilder;
        final Object graphToUseForMapper = config.get(TOKEN_USE_MAPPER_FROM_GRAPH);
        if (graphToUseForMapper != null) {
            if (null == graphs) throw new IllegalStateException(String.format(
                    "No graphs have been provided to the serializer and therefore %s is not a valid configuration", TOKEN_USE_MAPPER_FROM_GRAPH));

            final Graph g = graphs.get(graphToUseForMapper.toString());
            if (null == g) throw new IllegalStateException(String.format(
                    "There is no graph named [%s] configured to be used in the %s setting",
                    graphToUseForMapper, TOKEN_USE_MAPPER_FROM_GRAPH));

            // a graph was found so use the mapper it constructs.  this allows graphson to be auto-configured with any
            // custom classes that the implementation allows for
            initialBuilder = g.io(GraphSONIo.build()).mapper();
        } else {
            // no graph was supplied so just use the default - this will likely be the case when using a graph
            // with no custom classes or a situation where the user needs complete control like when using two
            // distinct implementations each with their own custom classes.
            initialBuilder = GraphSONMapper.build();
        }

        addIoRegistries(config, initialBuilder);

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

            logger.warn("Response [{}] could not be serialized by {}.", responseMessage.toString(), AbstractGraphSONMessageSerializerV1d0.class.getName());
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

            logger.warn("Request [{}] could not be serialized by {}.", requestMessage.toString(), AbstractGraphSONMessageSerializerV1d0.class.getName());
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
            logger.warn("Request [{}] could not be deserialized by {}.", msg, AbstractGraphSONMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    @Override
    public ResponseMessage deserializeResponse(final ByteBuf msg) throws SerializationException {
        try {
            final byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);
            final Map<String, Object> responseData = mapper.readValue(payload, mapTypeReference);
            final Map<String, Object> status = (Map<String, Object>) responseData.get(SerTokens.TOKEN_STATUS);
            final Map<String, Object> result = (Map<String, Object>) responseData.get(SerTokens.TOKEN_RESULT);
            return ResponseMessage.build(UUID.fromString(responseData.get(SerTokens.TOKEN_REQUEST).toString()))
                    .code(ResponseStatusCode.getFromValue((Integer) status.get(SerTokens.TOKEN_CODE)))
                    .statusMessage(status.get(SerTokens.TOKEN_MESSAGE).toString())
                    .statusAttributes((Map<String, Object>) status.get(SerTokens.TOKEN_ATTRIBUTES))
                    .result(result.get(SerTokens.TOKEN_DATA))
                    .responseMetaData((Map<String, Object>) result.get(SerTokens.TOKEN_META))
                    .create();
        } catch (Exception ex) {
            logger.warn("Response [{}] could not be deserialized by {}.", msg, AbstractGraphSONMessageSerializerV1d0.class.getName());
            throw new SerializationException(ex);
        }
    }

    public final static class GremlinServerModule extends SimpleModule {
        public GremlinServerModule() {
            super("graphson-gremlin-server");
            addSerializer(JsonBuilder.class, new JsonBuilderJacksonSerializer());
            addSerializer(ResponseMessage.class, new ResponseMessageSerializer());
        }
    }

    public final static class JsonBuilderJacksonSerializer extends StdSerializer<JsonBuilder> {
        public JsonBuilderJacksonSerializer() {
            super(JsonBuilder.class);
        }

        @Override
        public void serialize(final JsonBuilder json, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            // the JSON from the builder will already be started/ended as array or object...just need to surround it
            // with appropriate chars to fit into the serialization pattern.
            jsonGenerator.writeRaw(":");
            jsonGenerator.writeRaw(json.toString());
            jsonGenerator.writeRaw(",");
        }
    }

    public final static class ResponseMessageSerializer extends StdSerializer<ResponseMessage> {
        public ResponseMessageSerializer() {
            super(ResponseMessage.class);
        }

        @Override
        public void serialize(final ResponseMessage responseMessage, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            ser(responseMessage, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final ResponseMessage responseMessage, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider,
                                      final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            ser(responseMessage, jsonGenerator, serializerProvider, typeSerializer);
        }

        public void ser(final ResponseMessage responseMessage, final JsonGenerator jsonGenerator,
                        final SerializerProvider serializerProvider,
                        final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            jsonGenerator.writeStartObject();
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());

            jsonGenerator.writeStringField(SerTokens.TOKEN_REQUEST, responseMessage.getRequestId() != null ? responseMessage.getRequestId().toString() : null);
            jsonGenerator.writeObjectFieldStart(SerTokens.TOKEN_STATUS);

            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            jsonGenerator.writeStringField(SerTokens.TOKEN_MESSAGE, responseMessage.getStatus().getMessage());
            jsonGenerator.writeNumberField(SerTokens.TOKEN_CODE, responseMessage.getStatus().getCode().getValue());
            jsonGenerator.writeObjectField(SerTokens.TOKEN_ATTRIBUTES, responseMessage.getStatus().getAttributes());
            jsonGenerator.writeEndObject();

            jsonGenerator.writeObjectFieldStart(SerTokens.TOKEN_RESULT);
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            if (null == responseMessage.getResult().getData())
                jsonGenerator.writeNullField(SerTokens.TOKEN_DATA);
            else
                GraphSONUtil.writeWithType(SerTokens.TOKEN_DATA, responseMessage.getResult().getData(), jsonGenerator, serializerProvider, typeSerializer);
            jsonGenerator.writeObjectField(SerTokens.TOKEN_META, responseMessage.getResult().getMeta());
            jsonGenerator.writeEndObject();

            jsonGenerator.writeEndObject();
        }
    }
}
