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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.databind.util.StdDateFormat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Serializer tests that cover non-lossy serialization/deserialization methods.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONMessageSerializerGremlinTestV2d0 {

    private UUID requestId = UUID.fromString("6457272A-4018-4538-B9AE-08DD5DDC0AA1");
    private ResponseMessage.Builder responseMessageBuilder = ResponseMessage.build(requestId);
    private static ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    public MessageSerializer serializer = new GraphSONMessageSerializerGremlinV2d0();

    @Test
    public void shouldSerializeIterable() throws Exception {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(100);

        final ResponseMessage response = convert(list);
        assertCommon(response);

        final List<Integer> deserializedFunList = (List<Integer>) response.getResult().getData();
        assertEquals(2, deserializedFunList.size());
        assertEquals(new Integer(1), deserializedFunList.get(0));
        assertEquals(new Integer(100), deserializedFunList.get(1));
    }

    @Test
    public void shouldSerializeIterableWithNull() throws Exception {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(null);
        list.add(100);

        final ResponseMessage response = convert(list);
        assertCommon(response);

        final List<Integer> deserializedFunList = (List<Integer>) response.getResult().getData();
        assertEquals(3, deserializedFunList.size());
        assertEquals(new Integer(1), deserializedFunList.get(0));
        assertNull(deserializedFunList.get(1));
        assertEquals(new Integer(100), deserializedFunList.get(2));
    }

    @Test
    public void shouldSerializeMap() throws Exception {
        final Map<String, Object> map = new HashMap<>();
        final Map<String, String> innerMap = new HashMap<>();
        innerMap.put("a", "b");

        map.put("x", 1);
        map.put("y", "some");
        map.put("z", innerMap);

        final ResponseMessage response = convert(map);
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
    public void shouldSerializeMapEntries() throws Exception {
        final Graph graph = TinkerGraph.open();
        final Vertex v1 = graph.addVertex();
        final Date d = new Date();

        final Map<Object, Object> map = new HashMap<>();
        map.put("x", 1);
        map.put(v1, 100);
        map.put(d, "test");

        final ResponseMessage response = convert(IteratorUtils.asList(map.entrySet()));
        assertCommon(response);

        final List<Map<String, Object>> deserializedEntries = (List<Map<String, Object>>) response.getResult().getData();
        assertEquals(3, deserializedEntries.size());
        deserializedEntries.forEach(e -> {
            if (e.containsKey("x"))
                assertEquals(1, e.get("x"));
            else if (e.containsKey(v1.id().toString()))
                assertEquals(100, e.get(v1.id().toString()));
            else if (e.containsKey(StdDateFormat.instance.format(d)))
                assertEquals("test", e.get(StdDateFormat.instance.format(d)));
            else
                fail("Map entries contains a key that is not part of what was serialized");
        });
    }

    @Test
    public void shouldSerializeEdge() throws Exception {
        final Graph graph = TinkerGraph.open();
        final Vertex v1 = graph.addVertex();
        final Vertex v2 = graph.addVertex();
        final Edge e = v1.addEdge("test", v2);
        e.property("abc", 123);

        final Iterable<Edge> iterable = IteratorUtils.list(graph.edges());

        final ResponseMessage response = convert(iterable);
        assertCommon(response);

        final List<Edge> edgeList = (List<Edge>) response.getResult().getData();
        assertEquals(1, edgeList.size());

        final Edge deserializedEdge = edgeList.get(0);
        assertEquals(e.id(), deserializedEdge.id());
        assertEquals(v1.id(), deserializedEdge.outVertex().id());
        assertEquals(v2.id(), deserializedEdge.inVertex().id());
        assertEquals(v1.label(), deserializedEdge.outVertex().label());
        assertEquals(v2.label(), deserializedEdge.inVertex().label());
        assertEquals(e.label(), deserializedEdge.label());

        final List<Property> properties = new ArrayList<>();
        deserializedEdge.properties().forEachRemaining(properties::add);
        assertEquals(1, properties.size());

        assertNotNull(properties);
        assertEquals("abc", properties.get(0).key());
        assertEquals(123, properties.get(0).value());

    }

    @Test
    public void shouldSerializeEdgeProperty() throws Exception {
        final Graph graph = TinkerGraph.open();
        final Vertex v1 = graph.addVertex();
        final Vertex v2 = graph.addVertex();
        final Edge e = v1.addEdge("test", v2);
        e.property("abc", 123);

        final Iterable<Property<Object>> iterable = IteratorUtils.list(e.properties("abc"));
        final ResponseMessage response = convert(iterable);
        assertCommon(response);

        final List<Property> propertyList = (List<Property>) response.getResult().getData();
        assertEquals(1, propertyList.size());
        assertEquals(123, propertyList.get(0).value());
    }

    @Test
    public void shouldSerializeVertexWithEmbeddedMap() throws Exception {
        final Graph graph = TinkerGraph.open();
        final Vertex v = graph.addVertex();
        final Map<String, Object> map = new HashMap<>();
        map.put("x", 500);
        map.put("y", "some");

        final ArrayList<Object> friends = new ArrayList<>();
        friends.add("x");
        friends.add(5);
        friends.add(map);

        v.property(VertexProperty.Cardinality.single, "friends", friends);

        final List list = IteratorUtils.list(graph.vertices());

        final ResponseMessage response = convert(list);
        assertCommon(response);

        final List<Vertex> vertexList = (List<Vertex>) response.getResult().getData();
        assertEquals(1, vertexList.size());

        final Vertex deserializedVertex = vertexList.get(0);
        assertEquals(v.id(), deserializedVertex.id());
        assertEquals(Vertex.DEFAULT_LABEL, deserializedVertex.label());

        final List<VertexProperty> properties = new ArrayList<>();
        deserializedVertex.properties().forEachRemaining(properties::add);
        assertEquals(1, properties.size());

        final VertexProperty friendsProperty = properties.get(0);
        final List<Object> deserializedInnerList = (List<Object>) friendsProperty.value();

        assertEquals(3, deserializedInnerList.size());
        assertEquals("x", deserializedInnerList.get(0));
        assertEquals(5, deserializedInnerList.get(1));

        final Map<String, Object> deserializedInnerInnerMap = (Map<String, Object>) deserializedInnerList.get(2);
        assertEquals(2, deserializedInnerInnerMap.size());
        assertEquals(500, deserializedInnerInnerMap.get("x"));
        assertEquals("some", deserializedInnerInnerMap.get("y"));
    }

    @Test
    public void shouldSerializeToJsonMapWithElementForKey() throws Exception {
        final TinkerGraph graph = TinkerFactory.createClassic();
        final GraphTraversalSource g = graph.traversal();
        final Map<Vertex, Integer> map = new HashMap<>();
        map.put(g.V().has("name", "marko").next(), 1000);

        final ResponseMessage response = convert(map);
        assertCommon(response);

        final Map<String, Integer> deserializedMap = (Map<String, Integer>) response.getResult().getData();
        assertEquals(1, deserializedMap.size());

        // with no embedded types the key (which is a vertex) simply serializes out to an id
        // {"result":{"1":1000},"code":200,"requestId":"2d62161b-9544-4f39-af44-62ec49f9a595","type":0}
        assertEquals(new Integer(1000), deserializedMap.get("1"));
    }

    @Test
    public void shouldSerializeToTreeJson() throws Exception {
        final TinkerGraph graph = TinkerFactory.createClassic();
        final GraphTraversalSource g = graph.traversal();
        final Map t = g.V(1).out().properties("name").tree().next();

        final ResponseMessage response = convert(t);
        assertCommon(response);

        final Tree deserializedTree = (Tree)response.getResult().getData();

        //check the first object and its key's properties
        assertEquals(1, deserializedTree.size());
        Vertex v = ((Vertex) deserializedTree.keySet().iterator().next());
        assertEquals(1, v.id());
        assertEquals("marko", v.property("name").value());

        Tree firstTree = (Tree)deserializedTree.get(v);
        assertEquals(3, firstTree.size());
        Iterator<Vertex> vertexKeys = firstTree.keySet().iterator();

        Tree t2 = (Tree)firstTree.get(vertexKeys.next());
        VertexProperty vp = (VertexProperty)t2.keySet().iterator().next();
        assertEquals(3, vp.id());
        assertEquals("vadas", vp.value());

        t2 = (Tree) firstTree.get(vertexKeys.next());
        vp = (VertexProperty) t2.keySet().iterator().next();
        assertEquals(5, vp.id());
        assertEquals("lop", vp.value());

        t2 = (Tree) firstTree.get(vertexKeys.next());
        vp = (VertexProperty) t2.keySet().iterator().next();
        assertEquals(7, vp.id());
        assertEquals("josh", vp.value());
    }

    @Test
    public void shouldSerializeFullResponseMessage() throws Exception {
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

        final ByteBuf bb = serializer.serializeResponseAsBinary(response, allocator);
        final ResponseMessage deserialized = serializer.deserializeResponse(bb);

        assertEquals(id, deserialized.getRequestId());
        assertEquals("this", deserialized.getResult().getMeta().get("test"));
        assertEquals(1, deserialized.getResult().getMeta().get("one"));
        assertEquals("some-result", deserialized.getResult().getData());
        assertEquals("that", deserialized.getStatus().getAttributes().get("test"));
        assertEquals(2, deserialized.getStatus().getAttributes().get("two"));
        assertEquals(ResponseStatusCode.SUCCESS.getValue(), deserialized.getStatus().getCode().getValue());
        assertEquals("worked", deserialized.getStatus().getMessage());
    }
    
    private void assertCommon(final ResponseMessage response) {
        assertEquals(requestId, response.getRequestId());
        assertEquals(ResponseStatusCode.SUCCESS, response.getStatus().getCode());
    }

    private ResponseMessage convert(final Object toSerialize) throws SerializationException {
        final ByteBuf bb = serializer.serializeResponseAsBinary(responseMessageBuilder.result(toSerialize).create(), allocator);
        return serializer.deserializeResponse(bb);
    }
}
