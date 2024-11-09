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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tinkerpop.gremlin.util.MockitoHamcrestMatcherAdapter.reflectionEquals;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItemInArray;

public class GraphBinaryMessageSerializerV1Test {
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();

    @Test
    public void shouldSerializeAndDeserializeRequest() throws SerializationException {
        final GraphTraversalSource g = EmptyGraph.instance().traversal();
        final Traversal.Admin t = g.V().hasLabel("person").out().asAdmin();

        final Map<String, String> aliases = new HashMap<>();
        aliases.put("g","g");

        final RequestMessage request = RequestMessage.build(Tokens.OPS_BYTECODE)
                .processor("traversal")
                .overrideRequestId(UUID.randomUUID())
                .addArg(Tokens.ARGS_GREMLIN, t.getBytecode())
                .addArg(Tokens.ARGS_ALIASES, aliases)
                .create();

        final ByteBuf buffer = serializer.serializeRequestAsBinary(request, allocator);
        final int mimeLen = buffer.readByte();
        final byte[] bytes = new byte[mimeLen];
        buffer.readBytes(bytes);
        final String mimeType = new String(bytes, StandardCharsets.UTF_8);

        final RequestMessage deserialized = serializer.deserializeRequest(buffer);
        assertThat(request, reflectionEquals(deserialized));
        assertThat(serializer.mimeTypesSupported(), hasItemInArray(mimeType));
    }

    @Test
    public void shouldSerializeAndDeserializeRequestOverText() throws SerializationException {
        final RequestMessage request = RequestMessage.build("op1")
                .processor("proc1")
                .overrideRequestId(UUID.randomUUID())
                .addArg("arg1", "value1")
                .create();

        final String base64 = serializer.serializeRequestAsString(request, allocator);
        final RequestMessage deserialized = serializer.deserializeRequest(base64);
        assertThat(request, reflectionEquals(deserialized));
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
        assertThat(request, reflectionEquals(deserialized));
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
        assertThat(request, reflectionEquals(deserialized));
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
    public void shouldSerializeAndDeserializeResponseOverText() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build(UUID.randomUUID())
                .code(ResponseStatusCode.SUCCESS)
                .statusMessage("Found")
                .statusAttribute("k1", 1)
                .result("This is a fine message with a string")
                .create();

        final String base64 = serializer.serializeResponseAsString(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeResponse(base64);
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

    @Test
    public void shouldSupportConfigurationOfRegistryBuilder() {
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();
        final Map<String, Object> config = new HashMap<>();
        int counter = SampleTypeSerializerRegistryBuilder.createCounter.get();

        config.put(GraphBinaryMessageSerializerV1.TOKEN_BUILDER, "org.apache.tinkerpop.gremlin.util.ser.binary.GraphBinaryMessageSerializerV1Test$SampleTypeSerializerRegistryBuilder");
        serializer.configure(config, null);

        counter = SampleTypeSerializerRegistryBuilder.createCounter.get() - counter;
        // There should be a call to `create()`
        assertEquals(1, counter);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowWhenConfigurationOfRegistryBuilderFails() {
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();
        final Map<String, Object> config = new HashMap<>();
        config.put(GraphBinaryMessageSerializerV1.TOKEN_BUILDER, "org.apache.tinkerpop.gremlin.util.ser.binary.NonExistentClass");

        serializer.configure(config, null);
    }

    @Test
    public void shouldToStringSerialize() throws SerializationException {
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();
        final Map<String,Object> conf = new HashMap<String,Object>() {{
            put(GraphBinaryMessageSerializerV1.TOKEN_SERIALIZE_RESULT_TO_STRING, true);
        }};
        serializer.configure(conf, Collections.emptyMap());

        final ResponseMessage messageWithUnexpectedType = ResponseMessage.build(UUID.randomUUID()).
                result(java.awt.Color.RED).create();
        final ByteBuf buffer = serializer.serializeResponseAsBinary(messageWithUnexpectedType, allocator);
        final ResponseMessage deserialized = serializer.deserializeResponse(buffer);

        assertEquals(java.awt.Color.RED.toString(), deserialized.getResult().getData());
    }

    @Test
    public void shouldToStringSerializeAsText() throws SerializationException {
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();
        final Map<String,Object> conf = new HashMap<String,Object>() {{
            put(GraphBinaryMessageSerializerV1.TOKEN_SERIALIZE_RESULT_TO_STRING, true);
        }};
        serializer.configure(conf, Collections.emptyMap());

        final ResponseMessage messageWithUnexpectedType = ResponseMessage.build(UUID.randomUUID()).
                result(java.awt.Color.RED).create();
        final String base64 = serializer.serializeResponseAsString(messageWithUnexpectedType, allocator);
        final ResponseMessage deserialized = serializer.deserializeResponse(base64);

        assertEquals(java.awt.Color.RED.toString(), deserialized.getResult().getData());
    }

    @Test
    public void shouldSerializeAndDeserializeRequestAsText() throws SerializationException {
        final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1();
        final Map<String,Object> conf = new HashMap<String,Object>() {{
            put(GraphBinaryMessageSerializerV1.TOKEN_SERIALIZE_RESULT_TO_STRING, true);
        }};
        serializer.configure(conf, Collections.emptyMap());

        final RequestMessage request = RequestMessage.build("op1")
                .processor("proc1")
                .overrideRequestId(UUID.randomUUID())
                .addArg("arg1", "value1")
                .create();

        final ByteBuf buffer = serializer.serializeRequestAsBinary(request, allocator);
        final int mimeLen = buffer.readByte();
        buffer.readBytes(new byte[mimeLen]);
        final RequestMessage deserialized = serializer.deserializeRequest(buffer);
        assertThat(request, reflectionEquals(deserialized));
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

    public static class SampleTypeSerializerRegistryBuilder extends TypeSerializerRegistry.Builder {
        public static AtomicInteger createCounter = new AtomicInteger();

        @Override
        public TypeSerializerRegistry create() {
            createCounter.incrementAndGet();
            return super.create();
        }
    }
}
