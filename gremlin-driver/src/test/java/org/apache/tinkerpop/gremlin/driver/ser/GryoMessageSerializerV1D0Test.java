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
package org.apache.tinkerpop.gremlin.driver.ser;

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Serializer tests that cover non-lossy serialization/deserialization methods.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GryoMessageSerializerV1D0Test {
    private static final Map<String, Object> config = new HashMap<String, Object>() {{
        put("serializeResultToString", true);
    }};

    private UUID requestId = UUID.fromString("6457272A-4018-4538-B9AE-08DD5DDC0AA1");
    private ResponseMessage.Builder responseMessageBuilder = ResponseMessage.build(requestId);
    private static ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    public MessageSerializer binarySerializer = new GryoMessageSerializerV1d0();

    public MessageSerializer textSerializer = new GryoMessageSerializerV1d0();

    public GryoMessageSerializerV1D0Test() {
        textSerializer.configure(config, null);
    }

    @Test
    public void serializeIterable() throws Exception {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(100);

        final ResponseMessage response = convertBinary(list);
        assertCommon(response);

        final List<Integer> deserializedFunList = (List<Integer>) response.getResult().getData();
        assertEquals(2, deserializedFunList.size());
        assertEquals(new Integer(1), deserializedFunList.get(0));
        assertEquals(new Integer(100), deserializedFunList.get(1));
    }

    @Test
    public void serializeIterableToString() throws Exception {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(100);

        final ResponseMessage response = convertText(list);
        assertCommon(response);

        final List deserializedFunList = (List) response.getResult().getData();
        assertEquals(2, deserializedFunList.size());
        assertEquals("1", deserializedFunList.get(0));
        assertEquals("100", deserializedFunList.get(1));
    }

    @Test
    public void serializeIterableToStringWithNull() throws Exception {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(null);
        list.add(100);

        final ResponseMessage response = convertText(list);
        assertCommon(response);

        final List deserializedFunList = (List) response.getResult().getData();
        assertEquals(3, deserializedFunList.size());
        assertEquals("1", deserializedFunList.get(0).toString());
        assertEquals("null", deserializedFunList.get(1).toString());
        assertEquals("100", deserializedFunList.get(2).toString());
    }

    @Test
    public void serializeIterableWithNull() throws Exception {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(null);
        list.add(100);

        final ResponseMessage response = convertBinary(list);
        assertCommon(response);

        final List<Integer> deserializedFunList = (List<Integer>) response.getResult().getData();
        assertEquals(3, deserializedFunList.size());
        assertEquals(new Integer(1), deserializedFunList.get(0));
        assertNull(deserializedFunList.get(1));
        assertEquals(new Integer(100), deserializedFunList.get(2));
    }

    @Test
    public void serializeMap() throws Exception {
        final Map<String, Object> map = new HashMap<>();
        final Map<String, String> innerMap = new HashMap<>();
        innerMap.put("a", "b");

        map.put("x", 1);
        map.put("y", "some");
        map.put("z", innerMap);

        final ResponseMessage response = convertBinary(map);
        assertCommon(response);

        final Map<String, Object> deserializedMap = (Map<String, Object>) response.getResult().getData();
        assertEquals(3, deserializedMap.size());
        assertEquals(1, deserializedMap.get("x"));
        assertEquals("some", deserializedMap.get("y"));

        final Map<String, String> deserializedInnerMap = (Map<String, String>) deserializedMap.get("z");
        assertEquals(1, deserializedInnerMap.size());
        assertEquals("b", deserializedInnerMap.get("a"));
    }

    @Test
    public void serializeEdge() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("test", v2);
        e.property("abc", 123);

        final Iterable<Edge> iterable = IteratorUtils.list(g.edges());

        final ResponseMessage response = convertBinary(iterable);
        assertCommon(response);

        final List<DetachedEdge> edgeList = (List<DetachedEdge>) response.getResult().getData();
        assertEquals(1, edgeList.size());

        final DetachedEdge deserializedEdge = edgeList.get(0);
        assertEquals(e.id(), deserializedEdge.id());
        assertEquals("test", deserializedEdge.label());

        assertEquals(123, deserializedEdge.values("abc").next());
        assertEquals(1, IteratorUtils.count(deserializedEdge.properties()));
        assertEquals(v1.id(), deserializedEdge.outVertex().id());
        assertEquals(Vertex.DEFAULT_LABEL, deserializedEdge.outVertex().label());
        assertEquals(v2.id(), deserializedEdge.inVertex().id());
        assertEquals(Vertex.DEFAULT_LABEL, deserializedEdge.inVertex().label());
    }

    @Test
    public void serializeVertexWithEmbeddedMap() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v = g.addVertex();
        final Map<String, Object> map = new HashMap<>();
        map.put("x", 500);
        map.put("y", "some");

        final ArrayList<Object> friends = new ArrayList<>();
        friends.add("x");
        friends.add(5);
        friends.add(map);

        v.property(VertexProperty.Cardinality.single, "friends", friends);

        final List list = IteratorUtils.list(g.vertices());

        final ResponseMessage response = convertBinary(list);
        assertCommon(response);

        final List<DetachedVertex> vertexList = (List<DetachedVertex>) response.getResult().getData();
        assertEquals(1, vertexList.size());

        final DetachedVertex deserializedVertex = vertexList.get(0);
        assertEquals(0l, deserializedVertex.id());
        assertEquals(Vertex.DEFAULT_LABEL, deserializedVertex.label());

        assertEquals(1, IteratorUtils.count(deserializedVertex.properties()));

        final List<Object> deserializedInnerList = (List<Object>) deserializedVertex.values("friends").next();
        assertEquals(3, deserializedInnerList.size());
        assertEquals("x", deserializedInnerList.get(0));
        assertEquals(5, deserializedInnerList.get(1));

        final Map<String, Object> deserializedInnerInnerMap = (Map<String, Object>) deserializedInnerList.get(2);
        assertEquals(2, deserializedInnerInnerMap.size());
        assertEquals(500, deserializedInnerInnerMap.get("x"));
        assertEquals("some", deserializedInnerInnerMap.get("y"));
    }

    @Test
    public void serializeToMapWithElementForKey() throws Exception {
        final TinkerGraph graph = TinkerFactory.createClassic();
        final GraphTraversalSource g = graph.traversal();
        final Map<Vertex, Integer> map = new HashMap<>();
        map.put(g.V().has("name", "marko").next(), 1000);

        final ResponseMessage response = convertBinary(map);
        assertCommon(response);

        final Map<Vertex, Integer> deserializedMap = (Map<Vertex, Integer>) response.getResult().getData();
        assertEquals(1, deserializedMap.size());

        final Vertex deserializedMarko = deserializedMap.keySet().iterator().next();
        assertEquals("marko", deserializedMarko.values("name").next().toString());
        assertEquals(1, deserializedMarko.id());
        assertEquals(Vertex.DEFAULT_LABEL, deserializedMarko.label());
        assertEquals(29, deserializedMarko.values("age").next());
        assertEquals(2, IteratorUtils.count(deserializedMarko.properties()));

        assertEquals(new Integer(1000), deserializedMap.values().iterator().next());
    }

    @Test
    public void serializeFullResponseMessage() throws Exception {
        final UUID id = UUID.randomUUID();

        final Map<String, Object> metaData = new HashMap<>();
        metaData.put("test", "this");
        metaData.put("one", 1);

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("test", "that");
        attributes.put("two", 2);

        final ResponseMessage response = ResponseMessage.build(id)
                .responseMetaData(metaData)
                .code(ResponseStatusCode.SUCCESS)
                .result("some-result")
                .statusAttributes(attributes)
                .statusMessage("worked")
                .create();

        final ByteBuf bb = binarySerializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = binarySerializer.deserializeResponse(bb);

        assertEquals(id, deserialized.getRequestId());
        assertEquals("this", deserialized.getResult().getMeta().get("test"));
        assertEquals(1, deserialized.getResult().getMeta().get("one"));
        assertEquals("some-result", deserialized.getResult().getData());
        assertEquals("that", deserialized.getStatus().getAttributes().get("test"));
        assertEquals(2, deserialized.getStatus().getAttributes().get("two"));
        assertEquals(ResponseStatusCode.SUCCESS.getValue(), deserialized.getStatus().getCode().getValue());
        assertEquals("worked", deserialized.getStatus().getMessage());
    }

    @Test
    public void serializeFullRequestMessage() throws Exception {
        final UUID id = UUID.randomUUID();

        final RequestMessage request = RequestMessage.build("try")
                .overrideRequestId(id)
                .processor("pro")
                .addArg("test", "this")
                .create();
        final ByteBuf bb = binarySerializer.serializeRequestAsBinary(request, allocator);
        final int mimeLen = bb.readByte();
        bb.readBytes(new byte[mimeLen]);
        final RequestMessage deserialized = binarySerializer.deserializeRequest(bb);

        assertEquals(id, deserialized.getRequestId());
        assertEquals("pro", deserialized.getProcessor());
        assertEquals("try", deserialized.getOp());
        assertEquals("this", deserialized.getArgs().get("test"));
    }

    private void assertCommon(final ResponseMessage response) {
        assertEquals(requestId, response.getRequestId());
        assertEquals(ResponseStatusCode.SUCCESS, response.getStatus().getCode());
    }

    private ResponseMessage convertBinary(final Object toSerialize) throws SerializationException {
        final ByteBuf bb = binarySerializer.serializeResponseAsBinary(responseMessageBuilder.result(toSerialize).create(), allocator);
        return binarySerializer.deserializeResponse(bb);
    }

    private ResponseMessage convertText(final Object toSerialize) throws SerializationException {
        final ByteBuf bb = textSerializer.serializeResponseAsBinary(responseMessageBuilder.result(toSerialize).create(), allocator);
        return textSerializer.deserializeResponse(bb);
    }
}
