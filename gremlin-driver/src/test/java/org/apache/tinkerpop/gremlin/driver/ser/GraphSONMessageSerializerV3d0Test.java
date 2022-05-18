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
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3d0;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Serializer tests that cover non-lossy serialization/deserialization methods.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@SuppressWarnings("unchecked")
public class GraphSONMessageSerializerV3d0Test {

    private final UUID requestId = UUID.fromString("6457272A-4018-4538-B9AE-08DD5DDC0AA1");
    private final ResponseMessage.Builder responseMessageBuilder = ResponseMessage.build(requestId);
    private final static ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    public final MessageSerializer<ObjectMapper> serializer = new GraphSONMessageSerializerV3d0();

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

        map.put("w", true);
        map.put("x", 1);
        map.put("y", "some");
        map.put("z", innerMap);

        final ResponseMessage response = convert(map);
        assertCommon(response);

        final Map<String, Object> deserializedMap = (Map<String, Object>) response.getResult().getData();
        assertEquals(4, deserializedMap.size());
        assertEquals(true, deserializedMap.get("w"));
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

        final List<Map<Object, Object>> deserializedEntries = (List<Map<Object, Object>>) response.getResult().getData();
        assertEquals(3, deserializedEntries.size());
        deserializedEntries.forEach(m -> {
            final Map.Entry<Object,Object> e = m.entrySet().iterator().next();
            if (e.getKey().equals("x"))
                assertEquals(1, e.getValue());
            else if (e.getKey() instanceof Vertex && e.getKey().equals(v1))
                assertEquals(100, e.getValue());
            else if (e.getKey() instanceof Date)
                assertEquals("test", e.getValue());
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
        final Vertex v1 = g.V().has("name", "marko").next();
        map.put(v1, 1000);

        final ResponseMessage response = convert(map);
        assertCommon(response);

        final Map<Vertex, Integer> deserializedMap = (Map<Vertex, Integer>) response.getResult().getData();
        assertEquals(1, deserializedMap.size());
        assertEquals(new Integer(1000), deserializedMap.get(v1));
    }

    @Test
    public void shouldSerializeToTreeJson() throws Exception {
        final TinkerGraph graph = TinkerFactory.createClassic();
        final GraphTraversalSource g = graph.traversal();
        final Tree t = g.V(1).out().properties("name").tree().next();

        final ResponseMessage response = convert(t);
        assertCommon(response);

        final Tree deserializedTree = (Tree)response.getResult().getData();

        //check the first object and its key's properties
        assertEquals(1, deserializedTree.size());
        final Vertex v = ((Vertex) deserializedTree.keySet().iterator().next());
        assertEquals(1, v.id());
        assertEquals("marko", v.property("name").value());

        final Tree firstTree = (Tree)deserializedTree.get(v);
        assertEquals(3, firstTree.size());
        Iterator<Vertex> vertexKeys = firstTree.keySet().iterator();

        Map expectedValues = new HashMap<Integer, String>() {{
            put(3, "vadas");
            put(5, "lop");
            put(7, "josh");
        }};
        
        Map actualValues = new HashMap<Integer, String>();
        for (int i = 0; i < 3; i++) {
            Tree t2 = (Tree)firstTree.get(vertexKeys.next());
            VertexProperty vp = (VertexProperty)t2.keySet().iterator().next();
            actualValues.put(vp.id(), vp.value());
        }

        assertEquals(expectedValues, actualValues);
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

    @Test
    public void shouldDeserializeNotPredicate() throws Exception {
        final String requestMessageWithP = "{\"requestId\":{\"@type\":\"g:UUID\",\"@value\":\"0397b9c0-ffab-470e-a6a8-644fc80c01d6\"},\"op\":\"bytecode\",\"processor\":\"traversal\",\"args\":{\"gremlin\":{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"V\"],[\"hasLabel\",\"person\"],[\"has\",\"age\",{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"not\",\"value\":{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"lte\",\"value\":{\"@type\":\"g:Int32\",\"@value\":10}}}}}]]}},\"aliases\":{\"g\":\"gmodern\"}}}";
        final ByteBuf bb = allocator.buffer(requestMessageWithP.length());
        bb.writeBytes(requestMessageWithP.getBytes());
        final RequestMessage m = serializer.deserializeRequest(bb);
        assertEquals("bytecode", m.getOp());
        assertNotNull(m.getArgs());
    }

    @Test
    public void shouldRegisterGremlinServerModuleAutomaticallyWithMapper() throws SerializationException {
        GraphSONMapper.Builder builder = GraphSONMapper.build().addCustomModule(GraphSONXModuleV3d0.build().create(false));
        GraphSONMessageSerializerV3d0 graphSONMessageSerializerV3d0 = new GraphSONMessageSerializerV3d0(builder);

        ResponseMessage rm = convert("hello", graphSONMessageSerializerV3d0);
        assertEquals(rm.getRequestId(), requestId);
        assertEquals(rm.getResult().getData(), "hello");
    }

    @Test
    public void shouldFailOnMessageSerializerWithMapperIfNoGremlinServerModule() {
        final GraphSONMapper.Builder builder = GraphSONMapper.build().addCustomModule(GraphSONXModuleV3d0.build().create(false));
        final GraphSONMessageSerializerV3d0 graphSONMessageSerializerV3d0 = new GraphSONMessageSerializerV3d0(builder.create());

        try {
            convert("hello", graphSONMessageSerializerV3d0);
            fail("Serialization should have failed since no GremlinServerModule registered.");
        } catch (SerializationException e) {
            assertTrue(e.getMessage().contains("Could not find a type identifier for the class"));
            assertTrue(e.getCause() instanceof JsonMappingException);
            assertTrue(e.getCause().getCause() instanceof IllegalArgumentException);
        }
    }

    private void assertCommon(final ResponseMessage response) {
        assertEquals(requestId, response.getRequestId());
        assertEquals(ResponseStatusCode.SUCCESS, response.getStatus().getCode());
    }

    private ResponseMessage convert(final Object toSerialize, MessageSerializer<?> serializer) throws SerializationException {
        final ByteBuf bb = serializer.serializeResponseAsBinary(responseMessageBuilder.result(toSerialize).create(), allocator);
        return serializer.deserializeResponse(bb);
    }

    private ResponseMessage convert(final Object toSerialize) throws SerializationException {
        return convert(toSerialize, this.serializer);
    }
}
