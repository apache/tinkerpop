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
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.util.MockitoHamcrestMatcherAdapter.reflectionEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Serializer tests that cover non-lossy serialization/deserialization methods.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@SuppressWarnings("unchecked")
public class GraphSONMessageSerializerV4Test extends GraphSONMessageSerializerV3Test {

    public final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();

    @Test
    public void shouldDeserializeChunkedResponseMessage() throws SerializationException {
        final UUID id = UUID.randomUUID();
        final ResponseMessage response = ResponseMessage.build(id)
                .code(ResponseStatusCode.SUCCESS)
                .result(Arrays.asList("start/end", 0))
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeResponseHeader(response, allocator);
        final ByteBuf bb1 = serializer.writeResponseChunk(Arrays.asList("chunk", 1), allocator);
        final ByteBuf bb2 = serializer.writeResponseChunk(Arrays.asList("chunk", 2), allocator);
        final ByteBuf bb3 = serializer.writeResponseFooter(response, allocator);

        final ByteBuf bbCombined = allocator.buffer(bb0.readableBytes() + bb1.readableBytes() +
                bb2.readableBytes() + bb3.readableBytes());

        bbCombined.writeBytes(bb0);
        bbCombined.writeBytes(bb1);
        bbCombined.writeBytes(bb2);
        bbCombined.writeBytes(bb3);

        final ResponseMessage deserialized = serializer.deserializeResponse(bbCombined);

        assertEquals(8, ((List)deserialized.getResult().getData()).size());
        assertEquals(200, deserialized.getStatus().getCode().getValue());
        assertEquals("OK", deserialized.getStatus().getMessage());
    }

    @Test
    public void shouldDeserializeChunkedResponseMessageWithError() throws SerializationException {
        final UUID id = UUID.randomUUID();
        final ResponseMessage response = ResponseMessage.build(id)
                .code(ResponseStatusCode.SUCCESS)
                .result(Arrays.asList("start/end", 0))
                .statusMessage("OK")
                .create();

        final ByteBuf bb0 = serializer.writeResponseHeader(response, allocator);
        final ByteBuf bb1 = serializer.writeResponseChunk(Arrays.asList("chunk", 1), allocator);
        final ByteBuf bb2 = serializer.writeResponseChunk(Arrays.asList("chunk", 2), allocator);
        final ByteBuf bb3 = serializer.writeError(response, allocator);

        final ByteBuf bbCombined = allocator.buffer(bb0.readableBytes() + bb1.readableBytes() +
                bb2.readableBytes() + bb3.readableBytes());

        bbCombined.writeBytes(bb0);
        bbCombined.writeBytes(bb1);
        bbCombined.writeBytes(bb2);
        bbCombined.writeBytes(bb3);

        final ResponseMessage deserialized = serializer.deserializeResponse(bbCombined);

        // 3 chunks without errors, 2 items in each
        assertEquals(6, ((List)deserialized.getResult().getData()).size());
        // error description is in trailing headers
        assertEquals(200, deserialized.getStatus().getCode().getValue());
        assertEquals("OK", deserialized.getStatus().getMessage());
    }

    private static String getString(final ByteBuf bb) {
        final String result =  bb.readCharSequence(bb.readableBytes(), CharsetUtil.UTF_8).toString();
        bb.resetReaderIndex();
        return result;
    }
}
