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
package org.apache.tinkerpop.gremlin.driver.ser.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GraphBinaryMessageSerializerV1Test {
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();

    @Test
    public void shouldSerializeAndDeserializeRequest() throws SerializationException {
        final RequestMessage request = RequestMessage.build("op1")
                .processor("proc1")
                .overrideRequestId(UUID.randomUUID())
                .addArg("arg1", "value1")
                .create();

        final ByteBuf buffer = serializer.serializeRequestAsBinary(request, allocator);
        final int mimeLen = buffer.readByte();
        buffer.readBytes(new byte[mimeLen]);
        final RequestMessage deserialized = serializer.deserializeRequest(buffer);
        assertThat(request, new ReflectionEquals(deserialized));
    }

    @Test
    public void shouldSerializeAndDeserializeRequestWithoutArgs() throws SerializationException {
        final RequestMessage request = RequestMessage.build("op1")
                .processor("proc1")
                .overrideRequestId(UUID.randomUUID())
                .create();

        final ByteBuf buffer = serializer.serializeRequestAsBinary(request, allocator);
        final int mimeLen = buffer.readByte();
        buffer.readBytes(new byte[mimeLen]);
        final RequestMessage deserialized = serializer.deserializeRequest(buffer);
        assertThat(request, new ReflectionEquals(deserialized));
    }

    @Test
    public void shouldSerializeAndDeserializeRequestWithUnsetProcessor() throws SerializationException {
        final RequestMessage request = RequestMessage.build("op1")
                .overrideRequestId(UUID.randomUUID())
                .addArg("k", 1)
                .create();

        final ByteBuf buffer = serializer.serializeRequestAsBinary(request, allocator);
        final int mimeLen = buffer.readByte();
        buffer.readBytes(new byte[mimeLen]);
        final RequestMessage deserialized = serializer.deserializeRequest(buffer);
        assertThat(request, new ReflectionEquals(deserialized));
    }

    @Test
    public void shouldSerializeAndDeserializeResponse() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                .code(ResponseStatusCode.SUCCESS)
                .statusMessage("Found")
                .statusAttribute("k1", 1)
                .result("This is a fine message with a string")
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeResponse(buffer);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeResponseWithoutStatusMessage() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                .code(ResponseStatusCode.SUCCESS)
                .statusAttribute("k1", 1)
                .result(123.3)
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeResponse(buffer);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeResponseWithoutStatusAttributes() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                .code(ResponseStatusCode.SUCCESS)
                .result(123.3)
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeResponse(buffer);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeResponseWithoutResult() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                .code(ResponseStatusCode.SERVER_ERROR)
                .statusMessage("Something happened on the server")
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeResponse(buffer);
        assertResponseEquals(response, deserialized);
    }

    private static void assertResponseEquals(ResponseMessage expected, ResponseMessage actual) {
        assertEquals(expected.getRequestId(), actual.getRequestId());
        // Status
        assertEquals(expected.getStatus().getCode(), actual.getStatus().getCode());
        assertEquals(expected.getStatus().getMessage(), actual.getStatus().getMessage());
        assertEquals(expected.getStatus().getAttributes(), actual.getStatus().getAttributes());
        // Result
        assertEquals(expected.getResult().getData(), actual.getResult().getData());
        assertEquals(expected.getResult().getMeta(), actual.getResult().getMeta());
    }
}
