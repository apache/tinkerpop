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
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class GraphBinaryMessageSerializerV4Test {

    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();

    @Test
    public void shouldSerializeAndDeserializeResponseInSingleChunk() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                .code(ResponseStatusCode.SUCCESS)
                .statusMessage("OK")
                .result(Arrays.asList(1, "test"))
                .create();

        final ByteBuf buffer = serializer.writeHeader(response, allocator);
        final ResponseMessage deserialized = serializer.readChunk(buffer, true);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeResponseInHeaderChunk() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
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
        final ResponseMessage response = ResponseMessage.build((UUID)null)
                .result(Arrays.asList(1, "test"))
                .code(ResponseStatusCode.SUCCESS)
                .statusMessage("OK")
                .create();

        final ByteBuf buffer = serializer.writeFooter(response, allocator);
        final ResponseMessage deserialized = serializer.readChunk(buffer, false);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeErrorResponseWithEmptyData() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                .code(ResponseStatusCode.FORBIDDEN)
                .statusMessage("FORBIDDEN")
                .create();

        final ByteBuf buffer = serializer.writeHeader(response, allocator);
        final ResponseMessage deserialized = serializer.readChunk(buffer, true);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeCompositeResponse() throws SerializationException {
        final List headerData = Arrays.asList(0, "header");
        final ResponseMessage header = ResponseMessage.buildV4(UUID.randomUUID())
                .result(headerData)
                .create();

        final List chunkData1 = Arrays.asList(1, "data1");
        final List chunkData2 = Arrays.asList(2, "data2");

        final List footerData = Arrays.asList(0xFF, "footer");
        final ResponseMessage footer = ResponseMessage.build((UUID)null)
                .result(footerData)
                .code(ResponseStatusCode.SUCCESS)
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);
        final ByteBuf bb1 = serializer.writeChunk(chunkData1, allocator);
        final ByteBuf bb2 = serializer.writeChunk(chunkData2, allocator);
        final ByteBuf bb3 = serializer.writeFooter(footer, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final ResponseMessage deserialized = serializer.readChunk(bbCombined, true);

        assertEquals(header.getRequestId(), deserialized.getRequestId());
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
        final ResponseMessage header = ResponseMessage.buildV4(UUID.randomUUID())
                .result(headerData)
                .create();

        final List chunkData1 = Arrays.asList(1, "data1");
        final List chunkData2 = Arrays.asList(2, "data2");

        final List footerData = Arrays.asList(0xFF, "footer");
        final ResponseMessage footer = ResponseMessage.build((UUID)null)
                .result(footerData)
                .code(ResponseStatusCode.SERVER_ERROR)
                .statusMessage("SERVER_ERROR")
                .create();

        final ByteBuf bb0 = serializer.writeHeader(header, allocator);
        final ByteBuf bb1 = serializer.writeChunk(chunkData1, allocator);
        final ByteBuf bb2 = serializer.writeChunk(chunkData2, allocator);
        final ByteBuf bb3 = serializer.writeErrorFooter(footer, allocator);

        final ByteBuf bbCombined = allocator.buffer().writeBytes(bb0).writeBytes(bb1).writeBytes(bb2).writeBytes(bb3);

        final ResponseMessage deserialized = serializer.readChunk(bbCombined, true);

        assertEquals(header.getRequestId(), deserialized.getRequestId());
        // Status
        assertEquals(footer.getStatus().getCode(), deserialized.getStatus().getCode());
        assertEquals(footer.getStatus().getMessage(), deserialized.getStatus().getMessage());
        // Result
        List<Integer> combinedData = new ArrayList<>();
        Stream.of(headerData, chunkData1, chunkData2).forEach(combinedData::addAll);
        assertEquals(combinedData, deserialized.getResult().getData());
    }


    // copy-paste because response format will be different
    private static void assertResponseEquals(final ResponseMessage expected, final ResponseMessage actual) {
        assertEquals(expected.getRequestId(), actual.getRequestId());
        // Status
        assertEquals(expected.getStatus().getCode(), actual.getStatus().getCode());
        assertEquals(expected.getStatus().getMessage(), actual.getStatus().getMessage());
        assertEquals(expected.getStatus().getAttributes(), actual.getStatus().getAttributes());
        // Result
        // null == empty List
        if (!isEmptyData(expected) && !isEmptyData(actual)) {
            assertEquals(expected.getResult().getData(), actual.getResult().getData());
        }
        assertEquals(expected.getResult().getMeta(), actual.getResult().getMeta());
    }

    private static boolean isEmptyData(final ResponseMessage responseMessage) {
        return responseMessage.getResult() == null || responseMessage.getResult().getData() == null ||
                ((List) responseMessage.getResult().getData()).isEmpty();
    }
}
