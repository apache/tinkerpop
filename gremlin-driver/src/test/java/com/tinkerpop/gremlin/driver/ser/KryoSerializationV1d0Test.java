package com.tinkerpop.gremlin.driver.ser;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.message.ResultType;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.cached.CachedVertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Serializer tests that cover non-lossy serialization/deserialization methods.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KryoSerializationV1d0Test {
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

    // todo: get the test below to pass then add more tests

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

        v.setProperty("friends", friends);

        final List list = g.V().toList();

        final ResponseMessage response = convert(list);
        assertCommon(response);

        final List<Map<String, Object>> vertexList = (List<Map<String, Object>>) response.getResult();
        assertEquals(1, vertexList.size());

        final CachedVertex deserializedVertex = (CachedVertex) vertexList.get(0);
        assertEquals(0l, deserializedVertex.getId());
        assertEquals(Element.DEFAULT_LABEL, deserializedVertex.getLabel());

        final Map<String,Property> properties = deserializedVertex.getProperties();
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
