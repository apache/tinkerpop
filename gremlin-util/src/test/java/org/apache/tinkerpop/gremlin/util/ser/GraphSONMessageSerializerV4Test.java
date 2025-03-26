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
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV4;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseResult;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.util.ser.SerTokens.TOKEN_DATA;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class GraphSONMessageSerializerV4Test {

    private final static ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
    private final ResponseMessage.Builder responseMessageBuilder = ResponseMessage.build();
    private final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldSerializeChunkedResponseMessage() throws SerializationException, JsonProcessingException {
        final ResponseMessage header = ResponseMessage.build()
                .result(Arrays.asList("header", 0))
                .create();

        final ResponseMessage footer = ResponseMessage.build()
                .result(Arrays.asList("footer", 3))
                .code(HttpResponseStatus.OK)
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);
        final ByteBuf bb1 = serializer.writeChunk(Arrays.asList("chunk", 1), allocator);
        final ByteBuf bb2 = serializer.writeChunk(Arrays.asList("chunk", 2), allocator);
        final ByteBuf bb3 = serializer.writeFooter(footer, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final String json = bbCombined.readCharSequence(bbCombined.readableBytes(), CharsetUtil.UTF_8).toString();

        final JsonNode node = mapper.readTree(json);

        assertEquals("header", node.get("result").get(TOKEN_DATA).get("@value").get(0).textValue());
        assertEquals("footer", node.get("result").get(TOKEN_DATA).get("@value").get(6).textValue());
        assertEquals(8, node.get("result").get(TOKEN_DATA).get("@value").size());
        assertEquals("OK", node.get("status").get("message").asText());
        assertEquals(200, node.get("status").get("code").asInt());

        // a message composed of all chunks must be deserialized
        bbCombined.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(bbCombined);
        assertEquals(200, deserialized.getStatus().getCode().code());
        assertEquals("OK", deserialized.getStatus().getMessage());
        assertEquals(8, ((List)deserialized.getResult().getData()).size());
    }

    @Test
    public void shouldSerializeResponseMessageWithoutData() throws SerializationException, JsonProcessingException {
        final ResponseMessage header = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);

        final String json = bb0.readCharSequence(bb0.readableBytes(), CharsetUtil.UTF_8).toString();

        final JsonNode node = mapper.readTree(json);

        assertEquals(0, node.get("result").get(TOKEN_DATA).get("@value").size());
        assertEquals("OK", node.get("status").get("message").asText());
        assertEquals(200, node.get("status").get("code").asInt());

        bb0.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(bb0);
        assertEquals(200, deserialized.getStatus().getCode().code());
        assertEquals("OK", deserialized.getStatus().getMessage());
        assertEquals(0, ((List)deserialized.getResult().getData()).size());
    }

    @Test
    public void shouldSerializeChunkedResponseMessageWithEmptyData() throws SerializationException, JsonProcessingException {
        final ResponseMessage header = ResponseMessage.build()
                .result(new ArrayList<>())
                .code(HttpResponseStatus.OK)
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);

        final String json = bb0.readCharSequence(bb0.readableBytes(), CharsetUtil.UTF_8).toString();

        final JsonNode node = mapper.readTree(json);

        assertEquals(0, node.get("result").get(TOKEN_DATA).get("@value").size());
        assertEquals("OK", node.get("status").get("message").asText());
        assertEquals(200, node.get("status").get("code").asInt());

        bb0.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(bb0);
        assertEquals(200, deserialized.getStatus().getCode().code());
        assertEquals("OK", deserialized.getStatus().getMessage());
        assertEquals(0, ((List)deserialized.getResult().getData()).size());
    }

    @Test
    public void shouldSerializeChunkedResponseMessageWithError() throws SerializationException, JsonProcessingException {
        final ResponseMessage header = ResponseMessage.build()
                .result(Arrays.asList("header", 0))
                .create();

        final ResponseMessage footer = ResponseMessage.build()
                .result(Arrays.asList("footer", 3))
                .code(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                .statusMessage("SERVER_ERROR")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);
        final ByteBuf bb1 = serializer.writeChunk(Arrays.asList("chunk", 1), allocator);
        final ByteBuf bb2 = serializer.writeChunk(Arrays.asList("chunk", 2), allocator);
        final ByteBuf bb3 = serializer.writeErrorFooter(footer, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final String json = bbCombined.readCharSequence(bbCombined.readableBytes(), CharsetUtil.UTF_8).toString();

        final JsonNode node = mapper.readTree(json);

        assertEquals("header", node.get("result").get(TOKEN_DATA).get("@value").get(0).textValue());
        // 6 items in first 3 chunks
        assertEquals(6, node.get("result").get(TOKEN_DATA).get("@value").size());
        assertEquals("SERVER_ERROR", node.get("status").get("message").asText());
        assertEquals(500, node.get("status").get("code").asInt());

        bbCombined.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(bbCombined);
        assertEquals(500, deserialized.getStatus().getCode().code());
        assertEquals("SERVER_ERROR", deserialized.getStatus().getMessage());
        assertEquals(6, ((List)deserialized.getResult().getData()).size());
    }

    @Test
    public void shouldConfigureIoRegistry() throws Exception {
        final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();
        final Map<String, Object> config = new HashMap<String, Object>() {{
            put(AbstractMessageSerializer.TOKEN_IO_REGISTRIES, Arrays.asList(ColorIoRegistry.class.getName()));
        }};

        serializer.configure(config, null);

        final ResponseMessage toSerialize = ResponseMessage.build().result(Collections.singletonList(Color.RED)).code(HttpResponseStatus.OK).create();
        final ByteBuf buffer = serializer.serializeResponseAsBinary(toSerialize, allocator);
        ResponseResult results = serializer.deserializeBinaryResponse(buffer).getResult();

        assertEquals(Color.RED, results.getData().get(0));
    }

    public static class ColorIoRegistry extends AbstractIoRegistry {
        public ColorIoRegistry() {
            register(GraphSONIo.class, null, new ColorSimpleModule());
        }
    }

    public static class ColorSimpleModule extends TinkerPopJacksonModule {
        public ColorSimpleModule() {
            super("color-fun");
            addSerializer(Color.class, new ColorSerializer());
            addDeserializer(Color.class, new ColorDeserializer());
        }

        @Override
        public Map<Class, String> getTypeDefinitions() {
            return new HashMap<Class, String>(){{
                put(Color.class, "color");
            }};
        }

        @Override
        public String getTypeNamespace() {
            return "java";
        }
    }

    public static class ColorSerializer extends StdSerializer<Color> {
        public ColorSerializer() {
            super(Color.class);
        }

        @Override
        public void serialize(final Color value, final JsonGenerator gen,
                              final SerializerProvider serializerProvider) throws IOException {
            gen.writeString(value.toString());
        }

        @Override
        public void serializeWithType(final Color value, final JsonGenerator gen,
                                      final SerializerProvider serializers, final TypeSerializer typeSer) throws IOException {
            typeSer.writeTypePrefixForScalar(value, gen);
            gen.writeString(value.toString());
            typeSer.writeTypeSuffixForScalar(value, gen);
        }
    }

    public static class ColorDeserializer extends StdDeserializer<Color> {
        public ColorDeserializer() {
            super(Color.class);
        }

        @Override
        public Color deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final String colorString = jsonParser.getText();
            return colorString.contains("r=255") ? Color.RED : null;
        }
    }

    @Test
    public void shouldRegisterGremlinServerModuleAutomaticallyWithMapper() throws SerializationException {
        GraphSONMapper.Builder builder = GraphSONMapper.build().version(GraphSONVersion.V4_0).addCustomModule(GraphSONXModuleV4.build());
        GraphSONMessageSerializerV4 graphSONMessageSerializerV4 = new GraphSONMessageSerializerV4(builder);

        ResponseMessage rm = convert("hello", graphSONMessageSerializerV4);
        assertEquals(rm.getResult().getData().get(0), "hello");
    }

    private ResponseMessage convert(final Object toSerialize, MessageSerializer<?> serializer) throws SerializationException {
        final ByteBuf bb = serializer.serializeResponseAsBinary(
                responseMessageBuilder.result(Collections.singletonList(toSerialize)).code(HttpResponseStatus.OK).create(), allocator);
        return serializer.deserializeBinaryResponse(bb);
    }
}
