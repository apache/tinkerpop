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
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class GraphSONMessageSerializerV4Test extends GraphSONMessageSerializerV3Test {

    private final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldSerializeChunkedResponseMessage() throws SerializationException, JsonProcessingException {
        final UUID id = UUID.randomUUID();
        final ResponseMessage header = ResponseMessage.buildV4(id)
                .result(Arrays.asList("header", 0))
                .create();

        final ResponseMessage footer = ResponseMessage.buildV4(id)
                .result(Arrays.asList("footer", 3))
                .code(ResponseStatusCode.SUCCESS)
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);
        final ByteBuf bb1 = serializer.writeChunk(Arrays.asList("chunk", 1), allocator);
        final ByteBuf bb2 = serializer.writeChunk(Arrays.asList("chunk", 2), allocator);
        final ByteBuf bb3 = serializer.writeFooter(footer, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final String json = bbCombined.readCharSequence(bbCombined.readableBytes(), CharsetUtil.UTF_8).toString();

        final JsonNode node = mapper.readTree(json);

        assertEquals("header", node.get("result").get("@value").get(0).textValue());
        assertEquals("footer", node.get("result").get("@value").get(6).textValue());
        assertEquals(8, node.get("result").get("@value").size());
        assertEquals("OK", node.get("status").get("message").asText());
        assertEquals(200, node.get("status").get("code").asInt());

        // a message composed of all chunks must be deserialized
        bbCombined.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeResponse(bbCombined);
        assertEquals(id, deserialized.getRequestId());
        assertEquals(200, deserialized.getStatus().getCode().getValue());
        assertEquals("OK", deserialized.getStatus().getMessage());
        assertEquals(8, ((List)deserialized.getResult().getData()).size());
    }

    @Test
    public void shouldSerializeResponseMessageWithoutData() throws SerializationException, JsonProcessingException {
        final UUID id = UUID.randomUUID();
        final ResponseMessage header = ResponseMessage.buildV4(id)
                .code(ResponseStatusCode.SUCCESS)
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);

        final String json = bb0.readCharSequence(bb0.readableBytes(), CharsetUtil.UTF_8).toString();

        final JsonNode node = mapper.readTree(json);

        assertEquals(0, node.get("result").get("@value").size());
        assertEquals("OK", node.get("status").get("message").asText());
        assertEquals(200, node.get("status").get("code").asInt());

        bb0.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeResponse(bb0);
        assertEquals(id, deserialized.getRequestId());
        assertEquals(200, deserialized.getStatus().getCode().getValue());
        assertEquals("OK", deserialized.getStatus().getMessage());
        assertEquals(0, ((List)deserialized.getResult().getData()).size());
    }

    @Test
    public void shouldSerializeChunkedResponseMessageWithEmptyData() throws SerializationException, JsonProcessingException {
        final UUID id = UUID.randomUUID();
        final ResponseMessage header = ResponseMessage.buildV4(id)
                .result(new ArrayList<>())
                .code(ResponseStatusCode.SUCCESS)
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);

        final String json = bb0.readCharSequence(bb0.readableBytes(), CharsetUtil.UTF_8).toString();

        final JsonNode node = mapper.readTree(json);

        assertEquals(0, node.get("result").get("@value").size());
        assertEquals("OK", node.get("status").get("message").asText());
        assertEquals(200, node.get("status").get("code").asInt());

        bb0.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeResponse(bb0);
        assertEquals(id, deserialized.getRequestId());
        assertEquals(200, deserialized.getStatus().getCode().getValue());
        assertEquals("OK", deserialized.getStatus().getMessage());
        assertEquals(0, ((List)deserialized.getResult().getData()).size());
    }

    @Test
    public void shouldSerializeChunkedResponseMessageWithError() throws SerializationException, JsonProcessingException {
        final UUID id = UUID.randomUUID();
        final ResponseMessage header = ResponseMessage.buildV4(id)
                .result(Arrays.asList("header", 0))
                .create();

        final ResponseMessage footer = ResponseMessage.buildV4(id)
                .result(Arrays.asList("footer", 3))
                .code(ResponseStatusCode.SERVER_ERROR)
                .statusMessage("SERVER_ERROR")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);
        final ByteBuf bb1 = serializer.writeChunk(Arrays.asList("chunk", 1), allocator);
        final ByteBuf bb2 = serializer.writeChunk(Arrays.asList("chunk", 2), allocator);
        final ByteBuf bb3 = serializer.writeErrorFooter(footer, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final String json = bbCombined.readCharSequence(bbCombined.readableBytes(), CharsetUtil.UTF_8).toString();

        final JsonNode node = mapper.readTree(json);

        assertEquals("header", node.get("result").get("@value").get(0).textValue());
        // 6 items in first 3 chunks
        assertEquals(6, node.get("result").get("@value").size());
        assertEquals("SERVER_ERROR", node.get("status").get("message").asText());
        assertEquals(500, node.get("status").get("code").asInt());

        bbCombined.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeResponse(bbCombined);
        assertEquals(id, deserialized.getRequestId());
        assertEquals(500, deserialized.getStatus().getCode().getValue());
        assertEquals("SERVER_ERROR", deserialized.getStatus().getMessage());
        assertEquals(6, ((List)deserialized.getResult().getData()).size());
    }

    @Override
    protected ResponseMessage convert(final Object toSerialize, MessageSerializer<?> serializer) throws SerializationException {
        final ByteBuf bb = serializer.serializeResponseAsBinary(responseMessageBuilder.result(toSerialize).create(), allocator);
        return serializer.deserializeResponse(bb);
    }

    @Override
    protected ResponseMessage convert(final Object toSerialize) throws SerializationException {
        return convert(toSerialize, this.serializer);
    }
}
