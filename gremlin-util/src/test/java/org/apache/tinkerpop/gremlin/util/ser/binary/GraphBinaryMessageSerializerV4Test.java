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
package org.apache.tinkerpop.gremlin.util.ser.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class GraphBinaryMessageSerializerV4Test {

    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();

    public static class SampleTypeSerializerRegistryBuilder extends TypeSerializerRegistry.Builder {
        public static AtomicInteger createCounter = new AtomicInteger();

        @Override
        public TypeSerializerRegistry create() {
            createCounter.incrementAndGet();
            return super.create();
        }
    }

    @Test
    public void shouldSerializeAndDeserializeResponseInSingleChunk() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .result(Arrays.asList(1, "test"))
                .create();

        final ByteBuf buffer = serializer.writeHeader(response, allocator);
        final ResponseMessage deserialized = serializer.readChunk(buffer, true);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeResponseInHeaderChunk() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build()
                .result(Arrays.asList(1, "test"))
                .create();

        final ByteBuf buffer = serializer.writeHeader(response, allocator);
        final ResponseMessage deserialized = serializer.readChunk(buffer, true);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeResponseInDataChunk() throws SerializationException {
        final List data = Arrays.asList(1, "test");
        final ByteBuf buffer = serializer.writeChunk(data, allocator);
        final ResponseMessage deserialized = serializer.readChunk(buffer, false);

        assertEquals(data, deserialized.getResult().getData());
    }

    @Test
    public void shouldSerializeAndDeserializeResponseInFooterChunk() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build()
                .result(Arrays.asList(1, "test"))
                .code(HttpResponseStatus.OK)
                .statusMessage("OK")
                .create();

        final ByteBuf buffer = serializer.writeFooter(response, allocator);
        final ResponseMessage deserialized = serializer.readChunk(buffer, false);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeErrorResponseWithEmptyData() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.FORBIDDEN)
                .statusMessage("FORBIDDEN")
                .create();

        final ByteBuf buffer = serializer.writeHeader(response, allocator);
        final ResponseMessage deserialized = serializer.readChunk(buffer, true);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeCompositeResponse() throws SerializationException {
        final List headerData = Arrays.asList(0, "header");
        final ResponseMessage header = ResponseMessage.build()
                .result(headerData)
                .create();

        final List chunkData1 = Arrays.asList(1, "data1");
        final List chunkData2 = Arrays.asList(2, "data2");

        final List footerData = Arrays.asList(0xFF, "footer");
        final ResponseMessage footer = ResponseMessage.build()
                .result(footerData)
                .code(HttpResponseStatus.OK)
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);
        final ByteBuf bb1 = serializer.writeChunk(chunkData1, allocator);
        final ByteBuf bb2 = serializer.writeChunk(chunkData2, allocator);
        final ByteBuf bb3 = serializer.writeFooter(footer, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final ResponseMessage deserialized = serializer.readChunk(bbCombined, true);

        // Status
        assertEquals(footer.getStatus().getCode(), deserialized.getStatus().getCode());
        assertEquals(footer.getStatus().getMessage(), deserialized.getStatus().getMessage());
        // Result
        List<Integer> combinedData = new ArrayList<>();
        Stream.of(headerData, chunkData1, chunkData2, footerData).forEach(combinedData::addAll);
        assertEquals(combinedData, deserialized.getResult().getData());
    }

    @Test
    public void shouldSerializeAndDeserializeCompositeResponseWithError() throws SerializationException {
        final List headerData = Arrays.asList(0, "header");
        final ResponseMessage header = ResponseMessage.build()
                .result(headerData)
                .create();

        final List chunkData1 = Arrays.asList(1, "data1");
        final List chunkData2 = Arrays.asList(2, "data2");

        final List footerData = Arrays.asList(0xFF, "footer");
        final ResponseMessage footer = ResponseMessage.build()
                .result(footerData)
                .code(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                .statusMessage("SERVER_ERROR")
                .exception("fire in data center")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);
        final ByteBuf bb1 = serializer.writeChunk(chunkData1, allocator);
        final ByteBuf bb2 = serializer.writeChunk(chunkData2, allocator);
        final ByteBuf bb3 = serializer.writeErrorFooter(footer, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final ResponseMessage deserialized = serializer.readChunk(bbCombined, true);

        // Status
        assertEquals(footer.getStatus().getCode(), deserialized.getStatus().getCode());
        assertEquals(footer.getStatus().getMessage(), deserialized.getStatus().getMessage());
        assertEquals(footer.getStatus().getException(), deserialized.getStatus().getException());
        // Result
        List<Integer> combinedData = new ArrayList<>();
        Stream.of(headerData, chunkData1, chunkData2).forEach(combinedData::addAll);
        assertEquals(combinedData, deserialized.getResult().getData());
    }

    @Test
    public void shouldSupportConfigurationOfRegistryBuilder() {
        final Map<String, Object> config = new HashMap<>();
        int counter = SampleTypeSerializerRegistryBuilder.createCounter.get();

        config.put(GraphBinaryMessageSerializerV4.TOKEN_BUILDER, "org.apache.tinkerpop.gremlin.util.ser.binary.GraphBinaryMessageSerializerV4Test$SampleTypeSerializerRegistryBuilder");
        serializer.configure(config, null);

        counter = SampleTypeSerializerRegistryBuilder.createCounter.get() - counter;
        // There should be a call to `create()`
        assertEquals(1, counter);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowWhenConfigurationOfRegistryBuilderFails() {
        final Map<String, Object> config = new HashMap<>();
        config.put(GraphBinaryMessageSerializerV4.TOKEN_BUILDER, "org.apache.tinkerpop.gremlin.util.ser.binary.NonExistentClass");

        serializer.configure(config, null);
    }

    // copy-paste because response format will be different
    private static void assertResponseEquals(final ResponseMessage expected, final ResponseMessage actual) {
        // Status
        if (expected.getStatus() != null && actual.getStatus() != null) {
            assertEquals(expected.getStatus().getCode(), actual.getStatus().getCode());
            assertEquals(expected.getStatus().getMessage(), actual.getStatus().getMessage());
        }
        // Result
        // null == empty List
        if (!isEmptyData(expected) && !isEmptyData(actual)) {
            assertEquals(expected.getResult().getData(), actual.getResult().getData());
        }
    }

    private static boolean isEmptyData(final ResponseMessage responseMessage) {
        return responseMessage.getResult() == null || responseMessage.getResult().getData() == null ||
                responseMessage.getResult().getData().isEmpty();
    }
}
