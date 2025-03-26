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
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.util.ser.SerTokens.TOKEN_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GraphSONUntypedMessageSerializerV4Test {

    private final ResponseMessage.Builder responseMessageBuilder = ResponseMessage.build();
    private final GraphSONUntypedMessageSerializerV4 serializer = new GraphSONUntypedMessageSerializerV4();
    private final static ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldSerializeChunkedResponseMessage() throws SerializationException, JsonProcessingException {
        final ResponseMessage header = ResponseMessage.build()
                .result(Arrays.asList("header", 0))
                .create();

        final ResponseMessage footer = ResponseMessage.build()
                .result(Arrays.asList("footer", 3))
                .code(HttpResponseStatus.OK)
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);
        final ByteBuf bb1 = serializer.writeChunk(Arrays.asList("chunk", 1), allocator);
        final ByteBuf bb2 = serializer.writeChunk(Arrays.asList("chunk", 2), allocator);
        final ByteBuf bb3 = serializer.writeFooter(footer, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final String json = bbCombined.readCharSequence(bbCombined.readableBytes(), CharsetUtil.UTF_8).toString();

        final JsonNode node = mapper.readTree(json);

        assertEquals("header", node.get("result").get(TOKEN_DATA).get(0).textValue());
        assertEquals("footer", node.get("result").get(TOKEN_DATA).get(6).textValue());
        assertEquals(8, node.get("result").get(TOKEN_DATA).size());
        assertNull(node.get("status").get("message"));
        assertEquals(200, node.get("status").get("code").asInt());

        // a message composed of all chunks must be deserialized
        bbCombined.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(bbCombined);
        assertEquals(200, deserialized.getStatus().getCode().code());
        assertEquals(null, deserialized.getStatus().getMessage());
        assertEquals(8, ((List)deserialized.getResult().getData()).size());
    }

    @Test
    public void shouldSerializeResponseMessageWithoutData() throws SerializationException, JsonProcessingException {
        final ResponseMessage header = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);

        final String json = bb0.readCharSequence(bb0.readableBytes(), CharsetUtil.UTF_8).toString();

        final JsonNode node = mapper.readTree(json);

        assertEquals(0, node.get("result").get(TOKEN_DATA).size());
        assertNull(node.get("status").get("message"));
        assertEquals(200, node.get("status").get("code").asInt());

        bb0.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(bb0);
        assertEquals(200, deserialized.getStatus().getCode().code());
        assertEquals(null, deserialized.getStatus().getMessage());
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

        assertEquals(0, node.get("result").get(TOKEN_DATA).size());
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

        assertEquals("header", node.get("result").get(TOKEN_DATA).get(0).textValue());
        // 6 items in first 3 chunks
        assertEquals(6, node.get("result").get(TOKEN_DATA).size());
        assertEquals("SERVER_ERROR", node.get("status").get("message").asText());
        assertEquals(500, node.get("status").get("code").asInt());

        bbCombined.resetReaderIndex();
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(bbCombined);
        assertEquals(500, deserialized.getStatus().getCode().code());
        assertEquals("SERVER_ERROR", deserialized.getStatus().getMessage());
        assertEquals(6, ((List)deserialized.getResult().getData()).size());
    }
}
