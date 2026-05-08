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

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.util.ser.SerTokens.TOKEN_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GraphSONUntypedMessageSerializerV4Test {

    private final ResponseMessage.Builder responseMessageBuilder = ResponseMessage.build();
    private final GraphSONUntypedMessageSerializerV4 serializer = new GraphSONUntypedMessageSerializerV4();
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

        final Buffer bb0 = serializer.writeHeader(header);
        final Buffer bb1 = serializer.writeChunk(Arrays.asList("chunk", 1));
        final Buffer bb2 = serializer.writeChunk(Arrays.asList("chunk", 2));
        final Buffer bb3 = serializer.writeFooter(footer);

        final byte[] combined = combineBuffers(bb0, bb1, bb2, bb3);
        final String json = new String(combined, StandardCharsets.UTF_8);

        final JsonNode node = mapper.readTree(json);

        assertEquals("header", node.get("result").get(TOKEN_DATA).get(0).textValue());
        assertEquals("footer", node.get("result").get(TOKEN_DATA).get(6).textValue());
        assertEquals(8, node.get("result").get(TOKEN_DATA).size());
        assertNull(node.get("status").get("message"));
        assertEquals(200, node.get("status").get("code").asInt());

        // a message composed of all chunks must be deserialized
        final NettyBufferFactory bufferFactory = new NettyBufferFactory();
        final Buffer combinedBuffer = bufferFactory.create(io.netty.buffer.Unpooled.wrappedBuffer(combined));
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(combinedBuffer);
        assertEquals(200, deserialized.getStatus().getCode().code());
        assertEquals(null, deserialized.getStatus().getMessage());
        assertEquals(8, ((List)deserialized.getResult().getData()).size());
    }

    @Test
    public void shouldSerializeResponseMessageWithoutData() throws SerializationException, JsonProcessingException {
        final ResponseMessage header = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .create();

        final Buffer bb0 = serializer.writeHeader(header);

        final byte[] bytes = readBufferBytes(bb0);
        final String json = new String(bytes, StandardCharsets.UTF_8);

        final JsonNode node = mapper.readTree(json);

        assertEquals(0, node.get("result").get(TOKEN_DATA).size());
        assertNull(node.get("status").get("message"));
        assertEquals(200, node.get("status").get("code").asInt());

        final NettyBufferFactory bufferFactory = new NettyBufferFactory();
        final Buffer buffer = bufferFactory.create(io.netty.buffer.Unpooled.wrappedBuffer(bytes));
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(buffer);
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

        final Buffer bb0 = serializer.writeHeader(header);

        final byte[] bytes = readBufferBytes(bb0);
        final String json = new String(bytes, StandardCharsets.UTF_8);

        final JsonNode node = mapper.readTree(json);

        assertEquals(0, node.get("result").get(TOKEN_DATA).size());
        assertEquals("OK", node.get("status").get("message").asText());
        assertEquals(200, node.get("status").get("code").asInt());

        final NettyBufferFactory bufferFactory = new NettyBufferFactory();
        final Buffer buffer = bufferFactory.create(io.netty.buffer.Unpooled.wrappedBuffer(bytes));
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(buffer);
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

        final Buffer bb0 = serializer.writeHeader(header);
        final Buffer bb1 = serializer.writeChunk(Arrays.asList("chunk", 1));
        final Buffer bb2 = serializer.writeChunk(Arrays.asList("chunk", 2));
        final Buffer bb3 = serializer.writeErrorFooter(footer);

        final byte[] combined = combineBuffers(bb0, bb1, bb2, bb3);
        final String json = new String(combined, StandardCharsets.UTF_8);

        final JsonNode node = mapper.readTree(json);

        assertEquals("header", node.get("result").get(TOKEN_DATA).get(0).textValue());
        // 6 items in first 3 chunks
        assertEquals(6, node.get("result").get(TOKEN_DATA).size());
        assertEquals("SERVER_ERROR", node.get("status").get("message").asText());
        assertEquals(500, node.get("status").get("code").asInt());

        final NettyBufferFactory bufferFactory = new NettyBufferFactory();
        final Buffer combinedBuffer = bufferFactory.create(io.netty.buffer.Unpooled.wrappedBuffer(combined));
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(combinedBuffer);
        assertEquals(500, deserialized.getStatus().getCode().code());
        assertEquals("SERVER_ERROR", deserialized.getStatus().getMessage());
        assertEquals(6, ((List)deserialized.getResult().getData()).size());
    }

    private static byte[] readBufferBytes(final Buffer buffer) {
        final byte[] bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
        return bytes;
    }

    private static byte[] combineBuffers(final Buffer... buffers) {
        int totalLen = 0;
        final byte[][] arrays = new byte[buffers.length][];
        for (int i = 0; i < buffers.length; i++) {
            arrays[i] = readBufferBytes(buffers[i]);
            totalLen += arrays[i].length;
        }
        final byte[] combined = new byte[totalLen];
        int offset = 0;
        for (byte[] arr : arrays) {
            System.arraycopy(arr, 0, combined, offset, arr.length);
            offset += arr.length;
        }
        return combined;
    }
}
