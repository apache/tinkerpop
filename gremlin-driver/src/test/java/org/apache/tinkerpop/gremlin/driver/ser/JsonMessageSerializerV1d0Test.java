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
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * These tests focus on message serialization and not "result" serialization as test specific to results (e.g.
 * vertices, edges, annotated values, etc.) are handled in the IO packages.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JsonMessageSerializerV1d0Test {

    public static final JsonMessageSerializerV1d0 SERIALIZER = new JsonMessageSerializerV1d0();
    private static final RequestMessage msg = RequestMessage.build("op")
            .overrideRequestId(UUID.fromString("2D62161B-9544-4F39-AF44-62EC49F9A595")).create();

    @Test
    public void serializeToJsonNullResultReturnsNull() throws Exception {
        final ResponseMessage message = ResponseMessage.build(msg).create();
        final String results = SERIALIZER.serializeResponseAsString(message);
        final JSONObject json = new JSONObject(results);
        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(SerTokens.TOKEN_REQUEST));
        assertEquals(JSONObject.NULL, json.getJSONObject(SerTokens.TOKEN_RESULT).get(SerTokens.TOKEN_DATA));
    }

    @Test
    public void serializeToJsonIterable() throws Exception {
        final ArrayList<FunObject> funList = new ArrayList<>();
        funList.add(new FunObject("x"));
        funList.add(new FunObject("y"));

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(funList).create());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(SerTokens.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONObject(SerTokens.TOKEN_RESULT).getJSONArray(SerTokens.TOKEN_DATA);

        assertEquals(2, converted.length());

        assertEquals("x", converted.get(0));
        assertEquals("y", converted.get(1));
    }

    @Test
    public void serializeToJsonIterator() throws Exception {
        final ArrayList<FunObject> funList = new ArrayList<>();
        funList.add(new FunObject("x"));
        funList.add(new FunObject("y"));

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(funList.iterator()).create());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(SerTokens.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONObject(SerTokens.TOKEN_RESULT).getJSONArray(SerTokens.TOKEN_DATA);

        assertEquals(2, converted.length());

        assertEquals("x", converted.get(0));
        assertEquals("y", converted.get(1));
    }

    @Test
    public void serializeToJsonIteratorNullElement() throws Exception {

        ArrayList<FunObject> funList = new ArrayList<>();
        funList.add(new FunObject("x"));
        funList.add(null);
        funList.add(new FunObject("y"));

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(funList.iterator()).create());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(SerTokens.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONObject(SerTokens.TOKEN_RESULT).getJSONArray(SerTokens.TOKEN_DATA);

        assertEquals(3, converted.length());

        assertEquals("x", converted.get(0));
        assertEquals(JSONObject.NULL, converted.opt(1));
        assertEquals("y", converted.get(2));
    }

    @Test
    public void serializeToJsonMap() throws Exception {
        final Map<String, Object> map = new HashMap<>();
        final Map<String, String> innerMap = new HashMap<>();
        innerMap.put("a", "b");

        map.put("x", new FunObject("x"));
        map.put("y", "some");
        map.put("z", innerMap);

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(map).create());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(SerTokens.TOKEN_REQUEST));
        final JSONObject jsonObject = json.getJSONObject(SerTokens.TOKEN_RESULT).getJSONObject(SerTokens.TOKEN_DATA);

        assertNotNull(jsonObject);
        assertEquals("some", jsonObject.optString("y"));
        assertEquals("x", jsonObject.optString("x"));

        final JSONObject innerJsonObject = jsonObject.optJSONObject("z");
        assertNotNull(innerJsonObject);
        assertEquals("b", innerJsonObject.optString("a"));
    }

    @Test
    public void serializeEdge() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("test", v2);
        e.property("abc", 123);

        final Iterable<Edge> iterable = IteratorUtils.list(g.edges());
        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(iterable).create());

        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(SerTokens.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONObject(SerTokens.TOKEN_RESULT).getJSONArray(SerTokens.TOKEN_DATA);

        assertNotNull(converted);
        assertEquals(1, converted.length());

        final JSONObject edgeAsJson = converted.optJSONObject(0);
        assertNotNull(edgeAsJson);

        assertEquals(((Long) e.id()).intValue(), edgeAsJson.get(GraphSONTokens.ID));  // lossy
        assertEquals(((Long) v1.id()).intValue(), edgeAsJson.get(GraphSONTokens.OUT));// lossy
        assertEquals(((Long) v2.id()).intValue(), edgeAsJson.get(GraphSONTokens.IN)); // lossy
        assertEquals(e.label(), edgeAsJson.get(GraphSONTokens.LABEL));
        assertEquals(GraphSONTokens.EDGE, edgeAsJson.get(GraphSONTokens.TYPE));

        final JSONObject properties = edgeAsJson.optJSONObject(GraphSONTokens.PROPERTIES);
        assertNotNull(properties);
        assertEquals(123, properties.getInt("abc"));
    }

    @Test
    public void serializeEdgeProperty() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("test", v2);
        e.property("abc", 123);

        final Iterable<Property<Object>> iterable = IteratorUtils.list(e.properties("abc"));
        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(iterable).create());

        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(SerTokens.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONObject(SerTokens.TOKEN_RESULT).getJSONArray(SerTokens.TOKEN_DATA);

        assertNotNull(converted);
        assertEquals(1, converted.length());

        final JSONObject propertyAsJson = converted.optJSONObject(0);
        assertNotNull(propertyAsJson);

        assertEquals(123, propertyAsJson.getInt("value"));
    }

    @Test
    public void serializeToJsonIteratorWithEmbeddedMap() throws Exception {
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
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(SerTokens.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONObject(SerTokens.TOKEN_RESULT).getJSONArray(SerTokens.TOKEN_DATA);

        assertNotNull(converted);
        assertEquals(1, converted.length());

        final JSONObject vertexAsJson = converted.optJSONObject(0);
        assertNotNull(vertexAsJson);

        final JSONObject properties = vertexAsJson.optJSONObject(GraphSONTokens.PROPERTIES);
        assertNotNull(properties);

        final JSONArray friendsProperty = properties.optJSONArray("friends");
        assertNotNull(friendsProperty);
        assertEquals(3, friends.size());

        final String object1 = friendsProperty.getJSONObject(0).getJSONArray(GraphSONTokens.VALUE).getString(0);
        assertEquals("x", object1);

        final int object2 = friendsProperty.getJSONObject(0).getJSONArray(GraphSONTokens.VALUE).getInt(1);
        assertEquals(5, object2);

        final JSONObject object3 = friendsProperty.getJSONObject(0).getJSONArray(GraphSONTokens.VALUE).getJSONObject(2);
        assertEquals(500, object3.getInt("x"));
        assertEquals("some", object3.getString("y"));
    }

    @Test
    public void serializeToJsonMapWithElementForKey() throws Exception {
        final TinkerGraph graph = TinkerFactory.createClassic();
        final GraphTraversalSource g = graph.traversal();
        final Map<Vertex, Integer> map = new HashMap<>();
        map.put(g.V().has("name", Compare.eq, "marko").next(), 1000);

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.build(msg).result(map).create());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(SerTokens.TOKEN_REQUEST));
        final JSONObject converted = json.getJSONObject(SerTokens.TOKEN_RESULT).getJSONObject(SerTokens.TOKEN_DATA);

        assertNotNull(converted);

        // with no embedded types the key (which is a vertex) simply serializes out to an id
        // {"result":{"1":1000},"code":200,"requestId":"2d62161b-9544-4f39-af44-62ec49f9a595","type":0}
        assertEquals(1000, converted.optInt("1"));
    }

    @Test
    public void deserializeRequestNicelyWithNoArgs() throws Exception {
        final UUID request = UUID.fromString("011CFEE9-F640-4844-AC93-034448AC0E80");
        final RequestMessage m = SERIALIZER.deserializeRequest(String.format("{\"requestId\":\"%s\",\"op\":\"eval\"}", request));
        assertEquals(request, m.getRequestId());
        assertEquals("eval", m.getOp());
        assertNotNull(m.getArgs());
        assertEquals(0, m.getArgs().size());
    }

    @Test
    public void deserializeRequestNicelyWithArgs() throws Exception {
        final UUID request = UUID.fromString("011CFEE9-F640-4844-AC93-034448AC0E80");
        final RequestMessage m = SERIALIZER.deserializeRequest(String.format("{\"requestId\":\"%s\",\"op\":\"eval\",\"args\":{\"x\":\"y\"}}", request));
        assertEquals(request, m.getRequestId());
        assertEquals("eval", m.getOp());
        assertNotNull(m.getArgs());
        assertEquals("y", m.getArgs().get("x"));
    }

    @Test(expected = SerializationException.class)
    public void deserializeRequestParseMessage() throws Exception {
        SERIALIZER.deserializeRequest("{\"requestId\":\"%s\",\"op\":\"eval\",\"args\":{\"x\":\"y\"}}");
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

    private class FunObject {
        private String val;

        public FunObject(String val) {
            this.val = val;
        }

        public String toString() {
            return this.val;
        }
    }
}
