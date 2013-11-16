package com.tinkerpop.gremlin.server;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultSerializerTest {

    private static final RequestMessage msg = new RequestMessage();
    static {
        msg.requestId = UUID.fromString("2D62161B-9544-4F39-AF44-62EC49F9A595");
    }

    @Test
    public void serializeToStringNull() throws Exception {
        final String results = ResultSerializer.TO_STRING_RESULT_SERIALIZER.serialize(null, new Context(msg, null, null, null, null));
        assertEquals("2d62161b-9544-4f39-af44-62ec49f9a595>>null", results);
    }

    @Test
    public void serializeToStringAVertex() throws Exception {
        final TinkerGraph g = TinkerFactory.createClassic();
        final Vertex v = g.query().has("name", Compare.EQUAL, "marko").vertices().iterator().next();
        final String results = ResultSerializer.TO_STRING_RESULT_SERIALIZER.serialize(v, new Context(msg, null, null, null, null));
        assertEquals("2d62161b-9544-4f39-af44-62ec49f9a595>>v[1]", results);
    }

    @Test
    public void serializeToJsonNullResultReturnsNull() throws Exception {
        final String results = ResultSerializer.JSON_RESULT_SERIALIZER.serialize(null, new Context(msg, null, null, null, null));
        final JSONObject json = new JSONObject(results);
        assertNotNull(json);
        assertEquals(msg.requestId.toString(), json.getString(ResultSerializer.JsonResultSerializer.TOKEN_REQUEST));
        assertEquals(JSONObject.NULL, json.get(ResultSerializer.JsonResultSerializer.TOKEN_RESULT));
    }

    @Test
    public void serializeToJsonNullJSONObjectResultReturnsNull() throws Exception {
        final String results = ResultSerializer.JSON_RESULT_SERIALIZER.serialize(JSONObject.NULL, new Context(msg, null, null, null, null));
        final JSONObject json = new JSONObject(results);
        assertNotNull(json);
        assertEquals(msg.requestId.toString(), json.getString(ResultSerializer.JsonResultSerializer.TOKEN_REQUEST));
        assertEquals(JSONObject.NULL, json.get(ResultSerializer.JsonResultSerializer.TOKEN_RESULT));
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

        final String results = ResultSerializer.JSON_RESULT_SERIALIZER.serialize(funList, new Context(msg, null, null, null, null));
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.requestId.toString(), json.getString(ResultSerializer.JsonResultSerializer.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(ResultSerializer.JsonResultSerializer.TOKEN_RESULT);

        assertEquals(2, converted.length());

        assertEquals("x", converted.get(0));
        assertEquals("y", converted.get(1));
    }

    @Test
    public void serializeToJsonIterator() throws Exception {
        final ArrayList<FunObject> funList = new ArrayList<>();
        funList.add(new FunObject("x"));
        funList.add(new FunObject("y"));

        final String results = ResultSerializer.JSON_RESULT_SERIALIZER.serialize(funList.iterator(), new Context(msg, null, null, null, null));
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.requestId.toString(), json.getString(ResultSerializer.JsonResultSerializer.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(ResultSerializer.JsonResultSerializer.TOKEN_RESULT);

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

        final String results = ResultSerializer.JSON_RESULT_SERIALIZER.serialize(funList.iterator(), new Context(msg, null, null, null, null));
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.requestId.toString(), json.getString(ResultSerializer.JsonResultSerializer.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(ResultSerializer.JsonResultSerializer.TOKEN_RESULT);

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

        final String results = ResultSerializer.JSON_RESULT_SERIALIZER.serialize(map, new Context(msg, null, null, null, null));
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.requestId.toString(), json.getString(ResultSerializer.JsonResultSerializer.TOKEN_REQUEST));
        final JSONObject jsonObject = json.getJSONObject(ResultSerializer.JsonResultSerializer.TOKEN_RESULT);

        assertNotNull(jsonObject);
        assertEquals("some", jsonObject.optString("y"));
        assertEquals("x", jsonObject.optString("x"));

        final JSONObject innerJsonObject = jsonObject.optJSONObject("z");
        assertNotNull(innerJsonObject);
        assertEquals("b", innerJsonObject.optString("a"));
    }

    @Test
    public void serializedHiddenProperties() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v = g.addVertex();
        v.setProperty("abc", 123);
        final Property withHidden = v.setProperty("xyz", 321);
        withHidden.setProperty("audit", "stephen");

        final Iterator iterable = g.query().vertices().iterator();
        final String results = ResultSerializer.JSON_RESULT_SERIALIZER.serialize(iterable, new Context(msg, null, null, null, null));
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.requestId.toString(), json.getString(ResultSerializer.JsonResultSerializer.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(ResultSerializer.JsonResultSerializer.TOKEN_RESULT);

        assertNotNull(converted);
        assertEquals(1, converted.length());

        final JSONObject vertexAsJson = converted.optJSONObject(0);
        assertNotNull(vertexAsJson);

        final JSONObject properties = vertexAsJson.optJSONObject(ResultSerializer.JsonResultSerializer.TOKEN_PROPERTIES);
        assertNotNull(properties);

        final JSONObject valAbcProperty = properties.optJSONObject("abc");
        assertNotNull(valAbcProperty);
        assertEquals(123, valAbcProperty.getInt(ResultSerializer.JsonResultSerializer.TOKEN_VALUE));

        final JSONObject valXyzProperty = properties.optJSONObject("xyz");
        assertNotNull(valXyzProperty);
        assertEquals(321, valXyzProperty.getInt(ResultSerializer.JsonResultSerializer.TOKEN_VALUE));

        final JSONObject hiddenProperties = valXyzProperty.getJSONObject(ResultSerializer.JsonResultSerializer.TOKEN_HIDDEN);
        assertNotNull(hiddenProperties);
        assertEquals("stephen", hiddenProperties.getJSONObject("audit").getString(ResultSerializer.JsonResultSerializer.TOKEN_VALUE));
    }

    @Test
    public void serializeToJsonIteratorWithEmbeddedMap() throws Exception {
        final Graph g = TinkerGraph.open();
        final Vertex v = g.addVertex();
        final Map<String, Object> map = new HashMap<>();
        map.put("x", 500);
        map.put("y", "some");

        final ArrayList friends = new ArrayList();
        friends.add("x");
        friends.add(5);
        friends.add(map);

        v.setProperty("friends", friends);

        final Iterator iterable = g.query().vertices().iterator();
        final String results = ResultSerializer.JSON_RESULT_SERIALIZER.serialize(iterable, new Context(msg, null, null, null, null));
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.requestId.toString(), json.getString(ResultSerializer.JsonResultSerializer.TOKEN_REQUEST));
        final JSONArray converted = json.getJSONArray(ResultSerializer.JsonResultSerializer.TOKEN_RESULT);

        assertNotNull(converted);
        assertEquals(1, converted.length());

        final JSONObject vertexAsJson = converted.optJSONObject(0);
        assertNotNull(vertexAsJson);

        final JSONObject properties = vertexAsJson.optJSONObject(ResultSerializer.JsonResultSerializer.TOKEN_PROPERTIES);
        assertNotNull(properties);

        final JSONArray friendsProperty = properties.optJSONObject("friends").optJSONArray(ResultSerializer.JsonResultSerializer.TOKEN_VALUE);
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
        map.put(g.query().has("name", Compare.EQUAL, "marko").vertices().iterator().next(), 1000);

        final String results = ResultSerializer.JSON_RESULT_SERIALIZER.serialize(map, new Context(msg, null, null, null, null));
        final JSONObject json = new JSONObject(results);

        assertNotNull(json);
        assertEquals(msg.requestId.toString(), json.getString(ResultSerializer.JsonResultSerializer.TOKEN_REQUEST));
        final JSONObject converted = json.getJSONObject(ResultSerializer.JsonResultSerializer.TOKEN_RESULT);

        assertNotNull(converted);

        final JSONObject mapValue = converted.optJSONObject("1");
        assertEquals(1000, mapValue.optInt(ResultSerializer.JsonResultSerializer.TOKEN_VALUE));

        final JSONObject element = mapValue.optJSONObject(ResultSerializer.JsonResultSerializer.TOKEN_KEY);
        assertNotNull(element);
        assertEquals("1", element.optString("id"));
        assertEquals(ResultSerializer.JsonResultSerializer.TOKEN_VERTEX, element.optString(ResultSerializer.JsonResultSerializer.TOKEN_TYPE));
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
