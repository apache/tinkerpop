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
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.node.NullNode;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.util.StdDateFormat;
import org.junit.Test;

import java.awt.Color;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * These tests focus on message serialization and not "result" serialization as test specific to results (e.g.
 * vertices, edges, annotated values, etc.) are handled in the IO packages.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONMessageSerializerV2d0Test {

    public static final GraphSONMessageSerializerV2d0 SERIALIZER = new GraphSONMessageSerializerV2d0();
    private static final RequestMessage msg = RequestMessage.build("op")
            .overrideRequestId(UUID.fromString("2D62161B-9544-4F39-AF44-62EC49F9A595")).create();
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldConfigureIoRegistry() throws Exception {
        final GraphSONMessageSerializerV1d0 serializer = new GraphSONMessageSerializerV1d0();
        final Map<String, Object> config = new HashMap<String, Object>() {{
            put(GryoMessageSerializerV1d0.TOKEN_IO_REGISTRIES, Arrays.asList(ColorIoRegistry.class.getName()));
        }};

        serializer.configure(config, null);

        final ResponseMessage toSerialize = ResponseMessage.build(UUID.fromString("2D62161B-9544-4F39-AF44-62EC49F9A595"))
                .result(Color.RED).create();
        final String results = serializer.serializeResponseAsString(toSerialize);
        final JsonNode json = mapper.readTree(results);
        assertNotNull(json);
        assertThat(json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA).booleanValue(), is(true));
    }

    @Test
    public void shouldSerializeToJsonNullResultReturnsNull() throws Exception {
        final ResponseMessage message = ResponseMessage.build(msg).create();
        final String results = SERIALIZER.serializeResponseAsString(message);
        final JsonNode json = mapper.readTree(results);
        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.path(SerTokens.TOKEN_REQUEST).asText());
        assertEquals(NullNode.getInstance(), json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA));
    }

    @Test
    public void shouldSerializeToJsonIterable() throws Exception {
        final ArrayList<FunObject> funList = new ArrayList<>();
        funList.add(new FunObject("x"));
        funList.add(new FunObject("y"));

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(funList).create());
        final JsonNode json = mapper.readTree(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.get(SerTokens.TOKEN_REQUEST).asText());
        final JsonNode converted = json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA);

        assertEquals(2, converted.size());

        assertEquals("x", converted.get(0).asText());
        assertEquals("y", converted.get(1).asText());
    }

    @Test
    public void shouldSerializeToJsonIterator() throws Exception {
        final ArrayList<FunObject> funList = new ArrayList<>();
        funList.add(new FunObject("x"));
        funList.add(new FunObject("y"));

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(funList).create());
        final JsonNode json = mapper.readTree(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.get(SerTokens.TOKEN_REQUEST).asText());
        final JsonNode converted = json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA);

        assertEquals(2, converted.size());

        assertEquals("x", converted.get(0).asText());
        assertEquals("y", converted.get(1).asText());
    }

    @Test
    public void shouldSerializeToJsonIteratorNullElement() throws Exception {

        final ArrayList<FunObject> funList = new ArrayList<>();
        funList.add(new FunObject("x"));
        funList.add(null);
        funList.add(new FunObject("y"));

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(funList.iterator()).create());
        final JsonNode json = mapper.readTree(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.get(SerTokens.TOKEN_REQUEST).asText());
        final JsonNode converted = json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA);

        assertEquals(3, converted.size());

        assertEquals("x", converted.get(0).asText());
        assertEquals(NullNode.getInstance(), converted.get(1));
        assertEquals("y", converted.get(2).asText());
    }

    @Test
    public void shouldSerializeToJsonMap() throws Exception {
        final Map<String, Object> map = new HashMap<>();
        final Map<String, String> innerMap = new HashMap<>();
        innerMap.put("a", "b");

        map.put("x", new FunObject("x"));
        map.put("y", "some");
        map.put("z", innerMap);

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(map).create());
        final JsonNode json = mapper.readTree(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.get(SerTokens.TOKEN_REQUEST).asText());
        final JsonNode jsonObject = json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA);

        assertNotNull(jsonObject);
        assertEquals("some", jsonObject.get("y").asText());
        assertEquals("x", jsonObject.get("x").asText());

        final JsonNode innerJsonObject = jsonObject.get("z");
        assertNotNull(innerJsonObject);
        assertEquals("b", innerJsonObject.get("a").asText());
    }

    @Test
    public void shouldShouldSerializeMapEntries() throws Exception {
        final Graph graph = TinkerGraph.open();
        final Vertex v1 = graph.addVertex();
        final Date d = new Date();

        final Map<Object, Object> map = new HashMap<>();
        map.put("x", 1);
        map.put(v1, 100);
        map.put(d, "test");

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(IteratorUtils.asList(map)).create());
        final JsonNode json = mapper.readTree(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.get(SerTokens.TOKEN_REQUEST).asText());
        final JsonNode jsonObject = json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA);
        jsonObject.elements().forEachRemaining(e -> {
            if (e.has("x"))
                assertEquals(1, e.get("x").get(GraphSONTokens.VALUEPROP).asInt());
            else if (e.has(v1.id().toString()))
                assertEquals(100, e.get(v1.id().toString()).get(GraphSONTokens.VALUEPROP).asInt());
            else if (e.has(StdDateFormat.instance.format(d)))
                assertEquals("test", e.get(StdDateFormat.instance.format(d)).asText());
            else
                fail("Map entries contains a key that is not part of what was serialized");
        });
    }

    @Test
    public void shouldSerializeEdge() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("test", v2);
        e.property("abc", 123);

        final Iterable<Edge> iterable = IteratorUtils.list(g.edges());
        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(iterable).create());

        final JsonNode json = mapper.readTree(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.get(SerTokens.TOKEN_REQUEST).asText());
        final JsonNode converted = json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA);

        assertNotNull(converted);
        assertEquals(1, converted.size());

        final JsonNode edgeAsJson = converted.get(0).get(GraphSONTokens.VALUEPROP);
        assertNotNull(edgeAsJson);

        assertEquals(((Long) e.id()).longValue(), edgeAsJson.get(GraphSONTokens.ID).get(GraphSONTokens.VALUEPROP).asLong());
        assertEquals(((Long) v1.id()).longValue(), edgeAsJson.get(GraphSONTokens.OUT).get(GraphSONTokens.VALUEPROP).asLong());
        assertEquals(((Long) v2.id()).longValue(), edgeAsJson.get(GraphSONTokens.IN).get(GraphSONTokens.VALUEPROP).asLong());
        assertEquals(e.label(), edgeAsJson.get(GraphSONTokens.LABEL).asText());

        final JsonNode properties = edgeAsJson.get(GraphSONTokens.PROPERTIES);
        assertNotNull(properties);
        assertEquals(123, properties.get("abc").get(GraphSONTokens.VALUEPROP).get("value").get(GraphSONTokens.VALUEPROP).asInt());
    }

    @Test
    public void shouldSerializeEdgeProperty() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("test", v2);
        e.property("abc", 123);

        final Iterable<Property<Object>> iterable = IteratorUtils.list(e.properties("abc"));
        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(iterable).create());

        final JsonNode json = mapper.readTree(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.get(SerTokens.TOKEN_REQUEST).asText());
        final JsonNode converted = json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA);

        assertNotNull(converted);
        assertEquals(1, converted.size());

        final JsonNode propertyAsJson = converted.get(0);
        assertNotNull(propertyAsJson);

        assertEquals(123, propertyAsJson.get(GraphSONTokens.VALUEPROP).get("value").get(GraphSONTokens.VALUEPROP).asInt());
    }

    @Test
    public void shouldSerializeToJsonIteratorWithEmbeddedMap() throws Exception {
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

        final Iterable iterable = IteratorUtils.list(g.vertices());
        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(iterable).create());
        final JsonNode json = mapper.readTree(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.get(SerTokens.TOKEN_REQUEST).asText());
        final JsonNode converted = json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA);

        assertNotNull(converted);
        assertEquals(1, converted.size());

        final JsonNode vertexAsJson = converted.get(0).get(GraphSONTokens.VALUEPROP);
        assertNotNull(vertexAsJson);

        final JsonNode properties = vertexAsJson.get(GraphSONTokens.PROPERTIES);
        assertNotNull(properties);
        assertEquals(1, properties.size());

        final JsonNode friendProperties = properties.get("friends");
        assertEquals(1, friendProperties.size());
        final JsonNode friendsProperty = friendProperties.get(0).get(GraphSONTokens.VALUEPROP);
        assertNotNull(friendsProperty);
        assertEquals(3, friendsProperty.size());

        final String object1 = friendsProperty.get(GraphSONTokens.VALUE).get(0).asText();
        assertEquals("x", object1);

        final int object2 = friendsProperty.get(GraphSONTokens.VALUE).get(1).get(GraphSONTokens.VALUEPROP).asInt();
        assertEquals(5, object2);

        final JsonNode object3 = friendsProperty.get(GraphSONTokens.VALUE).get(2);
        assertEquals(500, object3.get("x").get(GraphSONTokens.VALUEPROP).asInt());
        assertEquals("some", object3.get("y").asText());
    }

    @Test
    public void shouldSerializeToJsonMapWithElementForKey() throws Exception {
        final TinkerGraph graph = TinkerFactory.createClassic();
        final GraphTraversalSource g = graph.traversal();
        final Map<Vertex, Integer> map = new HashMap<>();
        map.put(g.V().has("name", "marko").next(), 1000);

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(map).create());
        final JsonNode json = mapper.readTree(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.get(SerTokens.TOKEN_REQUEST).asText());
        final JsonNode converted = json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA);

        assertNotNull(converted);

        // with no embedded types the key (which is a vertex) simply serializes out to an id
        // {"result":{"1":1000},"code":200,"requestId":"2d62161b-9544-4f39-af44-62ec49f9a595","type":0}
        assertEquals(1000, converted.get("1").get(GraphSONTokens.VALUEPROP).asInt());
    }

    @Test
    public void shouldDeserializeRequestNicelyWithNoArgs() throws Exception {
        final UUID request = UUID.fromString("011CFEE9-F640-4844-AC93-034448AC0E80");
        final RequestMessage m = SERIALIZER.deserializeRequest(String.format("{\"requestId\":\"%s\",\"op\":\"eval\"}", request));
        assertEquals(request, m.getRequestId());
        assertEquals("eval", m.getOp());
        assertNotNull(m.getArgs());
        assertEquals(0, m.getArgs().size());
    }

    @Test
    public void shouldDeserializeRequestNicelyWithArgs() throws Exception {
        final UUID request = UUID.fromString("011CFEE9-F640-4844-AC93-034448AC0E80");
        final RequestMessage m = SERIALIZER.deserializeRequest(String.format("{\"requestId\":\"%s\",\"op\":\"eval\",\"args\":{\"x\":\"y\"}}", request));
        assertEquals(request, m.getRequestId());
        assertEquals("eval", m.getOp());
        assertNotNull(m.getArgs());
        assertEquals("y", m.getArgs().get("x"));
    }

    @Test(expected = SerializationException.class)
    public void shouldDeserializeRequestParseMessage() throws Exception {
        SERIALIZER.deserializeRequest("{\"requestId\":\"%s\",\"op\":\"eval\",\"args\":{\"x\":\"y\"}}");
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

        final String results = SERIALIZER.serializeResponseAsString(response);
        final ResponseMessage deserialized = SERIALIZER.deserializeResponse(results);

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
    public void shouldSerializeToTreeJson() throws Exception {
        final TinkerGraph graph = TinkerFactory.createClassic();
        final GraphTraversalSource g = graph.traversal();
        final Tree t = g.V(1).out().properties("name").tree().next();

        
        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(t).create());

        final JsonNode json = mapper.readTree(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.get(SerTokens.TOKEN_REQUEST).asText());
        final JsonNode converted = json.get(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA);
        assertNotNull(converted);

        //result is: tree{v1=>tree{vp['name'->'vadas']=>null, vp['name'->'lop']=>null, vp['name'->'josh']=>null}}
        
        //check the first object and it's properties
        assertEquals(1, converted.get(GraphSONTokens.VALUEPROP)
                .get(0)
                .get(GraphSONTokens.KEY).get(GraphSONTokens.VALUEPROP)
                .get(GraphSONTokens.ID).get(GraphSONTokens.VALUEPROP).asInt());

        assertEquals("marko", converted.get(GraphSONTokens.VALUEPROP)
                .get(0)
                .get(GraphSONTokens.KEY).get(GraphSONTokens.VALUEPROP)
                .get(GraphSONTokens.PROPERTIES)
                .get("name")
                .get(0).get(GraphSONTokens.VALUEPROP)
                .get(GraphSONTokens.VALUE).asText());

        //check the leafs
        assertEquals("vadas", converted.get(GraphSONTokens.VALUEPROP)
                .get(0)
                .get(GraphSONTokens.VALUE).get(GraphSONTokens.VALUEPROP)
                .get(0)
                .get(GraphSONTokens.KEY).get(GraphSONTokens.VALUEPROP)
                .get(GraphSONTokens.PROPERTIES)
                .get("name")
                .get(0).get(GraphSONTokens.VALUEPROP)
                .get(GraphSONTokens.VALUE).asText());

        assertEquals("lop", converted.get(GraphSONTokens.VALUEPROP)
                .get(0)
                .get(GraphSONTokens.VALUE).get(GraphSONTokens.VALUEPROP)
                .get(1)
                .get(GraphSONTokens.KEY).get(GraphSONTokens.VALUEPROP)
                .get(GraphSONTokens.PROPERTIES)
                .get("name")
                .get(0).get(GraphSONTokens.VALUEPROP)
                .get(GraphSONTokens.VALUE).asText());

        assertEquals("josh", converted.get(GraphSONTokens.VALUEPROP)
                .get(0)
                .get(GraphSONTokens.VALUE).get(GraphSONTokens.VALUEPROP)
                .get(2)
                .get(GraphSONTokens.KEY).get(GraphSONTokens.VALUEPROP)
                .get(GraphSONTokens.PROPERTIES)
                .get("name")
                .get(0).get(GraphSONTokens.VALUEPROP)
                .get(GraphSONTokens.VALUE).asText());
    }

    private class FunObject {
        private String val;

        public FunObject(String val) {
            this.val = val;
        }

        public String toString() {
            return this.val;
        }
    }

    public static class ColorIoRegistry extends AbstractIoRegistry {
        public ColorIoRegistry() {
            register(GraphSONIo.class, null, new ColorSimpleModule());
        }
    }

    public static class ColorSimpleModule extends SimpleModule {
        public ColorSimpleModule() {
            super("color-fun");
            addSerializer(Color.class, new ColorSerializer());

        }
    }

    public static class ColorSerializer extends StdSerializer<Color> {
        public ColorSerializer() {
            super(Color.class);
        }

        @Override
        public void serialize(final Color color, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            jsonGenerator.writeBoolean(color.equals(Color.RED));
        }
    }
}
