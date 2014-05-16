package com.tinkerpop.gremlin.driver.ser;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.message.ResultType;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.cached.CachedAnnotatedList;
import com.tinkerpop.gremlin.structure.util.cached.CachedAnnotatedValue;
import com.tinkerpop.gremlin.structure.util.cached.CachedEdge;
import com.tinkerpop.gremlin.structure.util.cached.CachedVertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
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
public class KryoMessageSerializerV1d0Test {
    private UUID requestId = UUID.fromString("6457272A-4018-4538-B9AE-08DD5DDC0AA1");
    private ResponseMessage.Builder responseMessageBuilder = ResponseMessage.create(requestId);
    private static ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    public MessageSerializer serializer = new KryoMessageSerializerV1d0();

    @Test
    public void serializeIterable() throws Exception {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(100);

        final ResponseMessage response = convert(list);
        assertCommon(response);

        final List<Integer> deserializedFunList = (List<Integer>) response.getResult();
        assertEquals(2, deserializedFunList.size());
        assertEquals(new Integer(1), deserializedFunList.get(0));
        assertEquals(new Integer(100), deserializedFunList.get(1));
    }

    @Test
    public void serializeIterableWithNull() throws Exception {
        final ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(null);
        list.add(100);

        final ResponseMessage response = convert(list);
        assertCommon(response);

        final List<Integer> deserializedFunList = (List<Integer>) response.getResult();
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

        final ResponseMessage response = convert(map);
        assertCommon(response);

        final Map<String, Object> deserializedMap = (Map<String, Object>) response.getResult();
        assertEquals(3, deserializedMap.size());
        assertEquals(1, deserializedMap.get("x"));
        assertEquals("some", deserializedMap.get("y"));

        final Map<String,String> deserializedInnerMap = (Map<String,String>) deserializedMap.get("z");
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

        final Iterable<Edge> iterable = g.E().toList();

        final ResponseMessage response = convert(iterable);
        assertCommon(response);

        final List<CachedEdge> edgeList = (List<CachedEdge>) response.getResult();
        assertEquals(1, edgeList.size());

        final CachedEdge deserialiedEdge = edgeList.get(0);
        assertEquals(2l, deserialiedEdge.id());
        assertEquals("test", deserialiedEdge.label());

        assertEquals(new Integer(123), (Integer) deserialiedEdge.value("abc"));
        assertEquals(1, deserialiedEdge.properties().size());
        assertEquals(0l, deserialiedEdge.outV().id().next());
        assertEquals(Element.DEFAULT_LABEL, deserialiedEdge.outV().label().next());
        assertEquals(1l, deserialiedEdge.inV().id().next());
        assertEquals(Element.DEFAULT_LABEL, deserialiedEdge.inV().label().next());
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

        v.property("friends", friends);

        final List list = g.V().toList();

        final ResponseMessage response = convert(list);
        assertCommon(response);

        final List<CachedVertex> vertexList = (List<CachedVertex>) response.getResult();
        assertEquals(1, vertexList.size());

        final CachedVertex deserializedVertex = vertexList.get(0);
        assertEquals(0l, deserializedVertex.id());
        assertEquals(Element.DEFAULT_LABEL, deserializedVertex.label());

        final Map<String,Property> properties = deserializedVertex.properties();
        assertEquals(1, properties.size());

        final List<Object> deserializedInnerList = (List<Object>) properties.get("friends").get();
        assertEquals(3, deserializedInnerList.size());
        assertEquals("x", deserializedInnerList.get(0));
        assertEquals(5, deserializedInnerList.get(1));

        final Map<String, Object> deserializedInnerInnerMap = (Map<String, Object>) deserializedInnerList.get(2);
        assertEquals(2, deserializedInnerInnerMap.size());
        assertEquals(500, deserializedInnerInnerMap.get("x"));
        assertEquals("some", deserializedInnerInnerMap.get("y"));
    }

    @Test
    public void serializeToJsonMapWithElementForKey() throws Exception {
        final TinkerGraph g = TinkerFactory.createClassic();
        final Map<Vertex, Integer> map = new HashMap<>();
        map.put(g.V().<Vertex>has("name", Compare.EQUAL, "marko").next(), 1000);

        final ResponseMessage response = convert(map);
        assertCommon(response);

        final Map<Vertex, Integer> deserializedMap = (Map<Vertex,Integer>) response.getResult();
        assertEquals(1, deserializedMap.size());

        final Vertex deserializedMarko = deserializedMap.keySet().iterator().next();
        assertEquals("marko", deserializedMarko.value("name").toString());
        assertEquals(1, deserializedMarko.id());
        assertEquals(Element.DEFAULT_LABEL, deserializedMarko.label());
        assertEquals(new Integer(29), (Integer) deserializedMarko.value("age"));
        assertEquals(2, deserializedMarko.properties().size());

        assertEquals(new Integer(1000), deserializedMap.values().iterator().next());
    }

    @Test
    public void serializeVertexWithAnnotatedList() throws Exception {
        final Graph g = TinkerFactory.createModern();
        final Vertex v = g.v(1);

        final ResponseMessage response = convert(v);
        assertCommon(response);

        final CachedVertex deserializedVertex = (CachedVertex) response.getResult();
        assertEquals(1, deserializedVertex.id());
        assertEquals("person", deserializedVertex.label());

        final Map<String,Property> properties = deserializedVertex.properties();
        assertEquals(2, properties.size());
        assertEquals("marko", deserializedVertex.<String>value("name"));

        final AnnotatedList<String> list = deserializedVertex.value("locations");
        assertEquals(4, list.values().count());

        list.annotatedValues().toList().forEach(av -> {
            if (av.getValue().equals("san diego")) {
                assertEquals(1997, av.getAnnotation("startTime").get());
                assertEquals(2001, av.getAnnotation("endTime").get());
            } else if (av.getValue().equals("santa cruz")) {
                assertEquals(2001, av.getAnnotation("startTime").get());
                assertEquals(2004, av.getAnnotation("endTime").get());
            } else if (av.getValue().equals("brussels")) {
                assertEquals(2004, av.getAnnotation("startTime").get());
                assertEquals(2005, av.getAnnotation("endTime").get());
            } else if (av.getValue().equals("santa fe")) {
                assertEquals(2005, av.getAnnotation("startTime").get());
                assertEquals(2014, av.getAnnotation("endTime").get());
            }

            assertEquals(2, av.getAnnotationKeys().size());
        });
    }

    @Test
    public void serializeAnnotatedList() throws Exception {
        final Graph g = TinkerFactory.createModern();
        final AnnotatedList<String> al = g.v(1).value("locations");

        final ResponseMessage response = convert(al);
        assertCommon(response);

        final CachedAnnotatedList<String> deserializedList = (CachedAnnotatedList<String>) response.getResult();
        assertEquals(4, deserializedList.values().count());

        deserializedList.annotatedValues().toList().forEach(av -> {
            if (av.getValue().equals("san diego")) {
                assertEquals(1997, av.getAnnotation("startTime").get());
                assertEquals(2001, av.getAnnotation("endTime").get());
            } else if (av.getValue().equals("santa cruz")) {
                assertEquals(2001, av.getAnnotation("startTime").get());
                assertEquals(2004, av.getAnnotation("endTime").get());
            } else if (av.getValue().equals("brussels")) {
                assertEquals(2004, av.getAnnotation("startTime").get());
                assertEquals(2005, av.getAnnotation("endTime").get());
            } else if (av.getValue().equals("santa fe")) {
                assertEquals(2005, av.getAnnotation("startTime").get());
                assertEquals(2014, av.getAnnotation("endTime").get());
            }

            assertEquals(2, av.getAnnotationKeys().size());
        });
    }

    @Test
    public void serializeAnnotatedValue() throws Exception {
        final Graph g = TinkerFactory.createModern();
        final AnnotatedList<String> al = g.v(1).value("locations");
        final AnnotatedValue<String> annotatedValue = al.annotatedValues().next();

        final ResponseMessage response = convert(annotatedValue);
        assertCommon(response);

        final CachedAnnotatedValue<String> av = (CachedAnnotatedValue<String>) response.getResult();

        assertEquals("san diego", av.getValue());
        assertEquals(1997, av.getAnnotation("startTime").get());
        assertEquals(2001, av.getAnnotation("endTime").get());
        assertEquals(2, av.getAnnotationKeys().size());
    }

    private void assertCommon(final ResponseMessage response) {
        assertEquals(requestId, response.getRequestId());
        assertEquals(ResultCode.SUCCESS, response.getCode());
        assertEquals(ResultType.OBJECT, response.getResultType());
    }

    private ResponseMessage convert(final Object toSerialize) throws SerializationException {
        final ByteBuf bb = serializer.serializeResponseAsBinary(responseMessageBuilder.result(toSerialize).build(), allocator);
        return serializer.deserializeResponse(bb);
    }
}
