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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.util.MockitoHamcrestMatcherAdapter.reflectionEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class MessageSerializerTest {
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {Serializers.GRAPHBINARY_V4, new GraphBinaryMessageSerializerV4()},
                {Serializers.GRAPHSON_V4, new GraphSONMessageSerializerV4()}
        });
    }

    @Parameterized.Parameter(0)
    public Serializers serializerType;

    @Parameterized.Parameter(1)
    public MessageSerializer serializer;

    @Test
    public void shouldSerializeAndDeserializeRequest() throws SerializationException {
        final GraphTraversalSource g = EmptyGraph.instance().traversal();
        final Traversal.Admin t = g.V().hasLabel("person").out().asAdmin();

        final Map<String, String> aliases = new HashMap<>();
        aliases.put("g","g");

        final RequestMessage request = RequestMessage.build(t.getGremlinLang().getGremlin())
                .addMaterializeProperties(Tokens.MATERIALIZE_PROPERTIES_TOKENS)
                .addTimeoutMillis(500)
                .addG("g1")
                .addLanguage("some-lang")
                .addBinding("k", "v")
                .create();

        final ByteBuf buffer = serializer.serializeRequestAsBinary(request, allocator);
        final RequestMessage deserialized = serializer.deserializeBinaryRequest(buffer);
        assertThat(request, reflectionEquals(deserialized));
    }

    @Test
    public void shouldSerializeAndDeserializeRequestWithoutArgs() throws SerializationException {
        final RequestMessage request = RequestMessage.build("query").create();

        final ByteBuf buffer = serializer.serializeRequestAsBinary(request, allocator);
        final RequestMessage deserialized = serializer.deserializeBinaryRequest(buffer);
        assertThat(request, reflectionEquals(deserialized));
    }

    @Test
    public void shouldSerializeAndDeserializeResponse() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .statusMessage("Found")
                .result(Collections.singletonList("This is a fine message with a string"))
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(buffer);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeResponseWithoutStatusMessage() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .result(Collections.singletonList(123.3))
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(buffer);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeResponseWithoutStatusAttributes() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.OK)
                .result(Collections.singletonList(123.3))
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(buffer);
        assertResponseEquals(response, deserialized);
    }

    @Test
    public void shouldSerializeAndDeserializeResponseWithoutResult() throws SerializationException {
        final ResponseMessage response = ResponseMessage.build()
                .code(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                .statusMessage("Something happened on the server")
                .create();

        final ByteBuf buffer = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeBinaryResponse(buffer);
        assertResponseEquals(response, deserialized);
    }

    private static void assertResponseEquals(ResponseMessage expected, ResponseMessage actual) {
        // Status
        assertEquals(expected.getStatus().getCode(), actual.getStatus().getCode());
        assertEquals(expected.getStatus().getMessage(), actual.getStatus().getMessage());
        assertEquals(expected.getStatus().getException(), actual.getStatus().getException());
        // Result
        assertEquals(expected.getResult().getData(), actual.getResult().getData());
    }
}
