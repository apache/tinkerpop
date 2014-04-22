package com.tinkerpop.gremlin.driver.ser;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JsonResultSerializerV1d0Test {

    public static final JsonMessageSerializerV1d0 SERIALIZER = new JsonMessageSerializerV1d0();
    private static final RequestMessage msg = RequestMessage.create("op")
            .overrideRequestId(UUID.fromString("2D62161B-9544-4F39-AF44-62EC49F9A595")).build();

    @Test
    public void serializeToJsonNullResultReturnsNull() throws Exception {
        final ResponseMessage message = ResponseMessage.create(msg).build();
        final String results = SERIALIZER.serializeResponseAsString(message);
        final JSONObject json = new JSONObject(results);
        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(JsonMessageSerializerV1d0.TOKEN_REQUEST));
        assertEquals(JSONObject.NULL, json.get(JsonMessageSerializerV1d0.TOKEN_RESULT));
    }

    @Test
    @Ignore("until we get Table/Row into pipes again.")
    public void serializeToJsonTableNotPaged() throws Exception {
        /*
        Table table = new Table("col1", "col2");
        table.addRow("x1", "x2");
        table.addRow("y1", "y2");

        JSONArray results = this.converterNotPaged.convert(table);

        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.length());

        boolean rowMatchX = false;
        boolean rowMatchY = false;
        for (int ix = 0; ix < results.length(); ix++) {
            JSONObject row = results.optJSONObject(ix);

            Assert.assertNotNull(row);
            Assert.assertTrue(row.has("col1"));
            Assert.assertTrue(row.has("col2"));

            if (row.optString("col1").equals("x1") && row.optString("col2").equals("x2")) {
                rowMatchX = true;
            }

            if (row.optString("col1").equals("y1") && row.optString("col2").equals("y2")) {
                rowMatchY = true;
            }
        }

        Assert.assertTrue(rowMatchX && rowMatchY);
        */
    }

    @Test
    public void serializeToJsonIterable() throws Exception {
        final ArrayList<FunObject> funList = new ArrayList<>();
        funList.add(new FunObject("x"));
        funList.add(new FunObject("y"));

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.create(msg).result(funList).build());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(JsonMessageSerializerV1d0.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(JsonMessageSerializerV1d0.TOKEN_RESULT);

        assertEquals(2, converted.length());

        assertEquals("x", converted.get(0));
        assertEquals("y", converted.get(1));
    }

    @Test
    public void serializeToJsonIterator() throws Exception {
        final ArrayList<FunObject> funList = new ArrayList<>();
        funList.add(new FunObject("x"));
        funList.add(new FunObject("y"));

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.create(msg).result(funList.iterator()).build());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(JsonMessageSerializerV1d0.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(JsonMessageSerializerV1d0.TOKEN_RESULT);

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

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.create(msg).result(funList.iterator()).build());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(JsonMessageSerializerV1d0.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(JsonMessageSerializerV1d0.TOKEN_RESULT);

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

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.create(msg).result(map).build());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(JsonMessageSerializerV1d0.TOKEN_REQUEST));
        final JSONObject jsonObject = json.getJSONObject(JsonMessageSerializerV1d0.TOKEN_RESULT);

        assertNotNull(jsonObject);
        assertEquals("some", jsonObject.optString("y"));
        assertEquals("x", jsonObject.optString("x"));

        final JSONObject innerJsonObject = jsonObject.optJSONObject("z");
        assertNotNull(innerJsonObject);
        assertEquals("b", innerJsonObject.optString("a"));
    }

    @Test
    public void serializePropertiesOnProperties() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v = g.addVertex();
        v.setProperty("abc", 123);
        ////// final Vertex.Property withMetaProperties = v.setProperty("xyz", 321);
        ///// withMetaProperties.setProperty("audit", "stephen");

        final Iterable iterable = g.V().toList();
        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.create(msg).result(iterable).build());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(JsonMessageSerializerV1d0.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(JsonMessageSerializerV1d0.TOKEN_RESULT);

        assertNotNull(converted);
        assertEquals(1, converted.length());

        final JSONObject vertexAsJson = converted.optJSONObject(0);
        assertNotNull(vertexAsJson);

        assertEquals(v.getId(), vertexAsJson.get(GraphSONTokens.ID));
        assertEquals(GraphSONTokens.VERTEX, vertexAsJson.get(GraphSONTokens.TYPE));

        final JSONObject properties = vertexAsJson.optJSONObject(GraphSONTokens.PROPERTIES);
        assertNotNull(properties);
        assertEquals(123, properties.getInt("abc"));

        /*
        final JSONObject valXyzProperty = properties.optJSONObject("xyz");
        assertNotNull(valXyzProperty);
        assertEquals(321, valXyzProperty.getInt(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_VALUE));

        final JSONObject metaProperties = valXyzProperty.getJSONObject(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_META);
        assertNotNull(metaProperties);
        assertEquals("stephen", metaProperties.getString("audit"));
        */
    }

    @Test
    public void serializeHiddenProperties() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v = g.addVertex("abc", 123);
        v.setProperty(Property.Key.hidden("hidden"), "stephen");

        final Iterable iterable = g.V().toList();
        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.create(msg).result(iterable).build());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(JsonMessageSerializerV1d0.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(JsonMessageSerializerV1d0.TOKEN_RESULT);

        assertNotNull(converted);
        assertEquals(1, converted.length());

        final JSONObject vertexAsJson = converted.optJSONObject(0);
        assertNotNull(vertexAsJson);

        assertEquals(v.getId(), vertexAsJson.get(GraphSONTokens.ID));
        assertEquals(GraphSONTokens.VERTEX, vertexAsJson.get(GraphSONTokens.TYPE));

        final JSONObject properties = vertexAsJson.optJSONObject(GraphSONTokens.PROPERTIES);
        assertNotNull(properties);

        assertEquals(123, properties.getInt("abc"));
        assertEquals("stephen", properties.getString(Property.Key.hidden("hidden")));
    }

    /*
    @Test
    @Ignore("How do we recognize multi-properties programmatically?")
    public void serializeMultiProperties() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v = g.addVertex("abc", 123);
        v.addProperty("multi", 1);
        v.addProperty("multi", 3);
        v.addProperty("multi", 2);

        final Iterator iterable = g.query().vertices().iterator();
        final String results = SERIALIZER.serializeResponseAsString(iterable, new Context(msg, null, null, null, null));
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.requestId.toString(), json.getString(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_RESULT);

        assertNotNull(converted);
        assertEquals(1, converted.length());

        final JSONObject vertexAsJson = converted.optJSONObject(0);
        assertNotNull(vertexAsJson);

        assertEquals(v.getId(), vertexAsJson.get(MessageSerializer.JsonMessageSerializerV1d0.ID));
        assertEquals(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_VERTEX, vertexAsJson.get(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_TYPE));

        final JSONObject properties = vertexAsJson.optJSONObject(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_PROPERTIES);
        assertNotNull(properties);

        final JSONObject valAbcProperty = properties.optJSONObject("abc");
        assertNotNull(valAbcProperty);
        assertEquals(123, valAbcProperty.getInt(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_VALUE));

        final JSONObject valHiddenProperty = properties.optJSONObject(Property.Key.hidden("multi"));
        assertNotNull(valHiddenProperty);
        assertEquals("stephen", valHiddenProperty.getString(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_VALUE));
    }
    */

    @Test
    public void serializeEdge() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v1 = g.addVertex();
        final Vertex v2 = g.addVertex();
        final Edge e = v1.addEdge("test", v2);
        e.setProperty("abc", 123);

        final Iterable<Edge> iterable = g.E().toList();
        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.create(msg).result(iterable).build());

        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(JsonMessageSerializerV1d0.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(JsonMessageSerializerV1d0.TOKEN_RESULT);

        assertNotNull(converted);
        assertEquals(1, converted.length());

        final JSONObject edgeAsJson = converted.optJSONObject(0);
        assertNotNull(edgeAsJson);

        assertEquals(e.getId(), edgeAsJson.get(GraphSONTokens.ID));
        assertEquals(v1.getId(), edgeAsJson.get(GraphSONTokens.OUT));
        assertEquals(v2.getId(), edgeAsJson.get(GraphSONTokens.IN));
        assertEquals(e.getLabel(), edgeAsJson.get(GraphSONTokens.LABEL));
        assertEquals(GraphSONTokens.EDGE, edgeAsJson.get(GraphSONTokens.TYPE));

        final JSONObject properties = edgeAsJson.optJSONObject(GraphSONTokens.PROPERTIES);
        assertNotNull(properties);
        assertEquals(123, properties.getInt("abc"));

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

        v.setProperty("friends", friends);

        final Iterable iterable = g.V().toList();
        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.create(msg).result(iterable).build());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(JsonMessageSerializerV1d0.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(JsonMessageSerializerV1d0.TOKEN_RESULT);

        assertNotNull(converted);
        assertEquals(1, converted.length());

        final JSONObject vertexAsJson = converted.optJSONObject(0);
        assertNotNull(vertexAsJson);

        final JSONObject properties = vertexAsJson.optJSONObject(GraphSONTokens.PROPERTIES);
        assertNotNull(properties);

        final JSONArray friendsProperty = properties.optJSONArray("friends");
        assertNotNull(friendsProperty);
        assertEquals(3, friends.size());

        final String object1 = friendsProperty.getString(0);
        assertEquals("x", object1);

        final int object2 = friendsProperty.getInt(1);
        assertEquals(5, object2);

        final JSONObject object3 = friendsProperty.getJSONObject(2);
        assertEquals(500, object3.getInt("x"));
        assertEquals("some", object3.getString("y"));
    }

    @Test
    public void serializeToJsonMapWithElementForKey() throws Exception {
        final TinkerGraph g = TinkerFactory.createClassic();
        final Map<Vertex, Integer> map = new HashMap<>();
        map.put(g.V().<Vertex>has("name", Compare.EQUAL, "marko").next(), 1000);

        final String results = SERIALIZER.serializeResponseAsString(ResponseMessage.create(msg).result(map).build());
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.getRequestId().toString(), json.getString(JsonMessageSerializerV1d0.TOKEN_REQUEST));
        final JSONObject converted = json.getJSONObject(JsonMessageSerializerV1d0.TOKEN_RESULT);

        assertNotNull(converted);

        // TODO: come back to fix this once we figure out how to nicely handle maps
        /*
        final JSONObject mapValue = converted.optJSONObject("1");
        assertEquals(1000, mapValue.optInt(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_VALUE));

        final JSONObject element = mapValue.optJSONObject(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_KEY);
        assertNotNull(element);
        assertEquals("1", element.optString("id"));
        assertEquals(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_VERTEX, element.optString(MessageSerializer.JsonMessageSerializerV1d0.TOKEN_TYPE));
        */
    }

    // todo: more TESTS!!

    @Test
    public void deserializeRequestNicelyWithNoArgs() {
        final UUID request = UUID.fromString("011CFEE9-F640-4844-AC93-034448AC0E80");
        final Optional<RequestMessage> msg = SERIALIZER.deserializeRequest(String.format("{\"requestId\":\"%s\",\"op\":\"eval\"}", request));
        assertTrue(msg.isPresent());

        final RequestMessage m = msg.get();
        assertEquals(request, m.getRequestId());
        assertEquals("eval", m.getOp());
        assertNotNull(m.getArgs());
        assertEquals(0, m.getArgs().size());
    }

    @Test
    public void deserializeRequestNicelyWithArgs() {
        final UUID request = UUID.fromString("011CFEE9-F640-4844-AC93-034448AC0E80");
        final Optional<RequestMessage> msg = SERIALIZER.deserializeRequest(String.format("{\"requestId\":\"%s\",\"op\":\"eval\",\"args\":{\"x\":\"y\"}}", request));
        assertTrue(msg.isPresent());

        final RequestMessage m = msg.get();
        assertEquals(request, m.getRequestId());
        assertEquals("eval", m.getOp());
        assertNotNull(m.getArgs());
        assertEquals("y", m.getArgs().get("x"));
    }

    @Test
    public void deserializeRequestParseMessage() {
        final Optional<RequestMessage> msg = SERIALIZER.deserializeRequest("{\"requestId\":\"%s\",\"op\":\"eval\",\"args\":{\"x\":\"y\"}}");
        assertFalse(msg.isPresent());
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
