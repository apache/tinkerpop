package com.tinkerpop.gremlin.server;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.server.util.ser.JsonMessageSerializerV1d0;
import com.tinkerpop.gremlin.server.util.ser.ToStringMessageSerializer;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MessageSerializerTest {

    private static final RequestMessage msg = new RequestMessage.Builder("op")
            .overrideRequestId(UUID.fromString("2D62161B-9544-4F39-AF44-62EC49F9A595")).build();

    @Test
    public void shouldGetTextSerializer() {
        final MessageSerializer serializer = MessageSerializer.select("text/plain", MessageSerializer.DEFAULT_RESULT_SERIALIZER);
        assertNotNull(serializer);
        assertTrue(ToStringMessageSerializer.class.isAssignableFrom(serializer.getClass()));
    }

    @Test
    public void shouldGetJsonSerializer() {
        final MessageSerializer serializerGremlinV1 = MessageSerializer.select("application/vnd.gremlin-v1.0+json", MessageSerializer.DEFAULT_RESULT_SERIALIZER);
        assertNotNull(serializerGremlinV1);
        assertTrue(JsonMessageSerializerV1d0.class.isAssignableFrom(serializerGremlinV1.getClass()));

        final MessageSerializer serializerJson = MessageSerializer.select("application/json", MessageSerializer.DEFAULT_RESULT_SERIALIZER);
        assertNotNull(serializerJson);
        assertTrue(JsonMessageSerializerV1d0.class.isAssignableFrom(serializerJson.getClass()));
    }

    @Test
    public void shouldGetTextSerializerAsDefault() {
        final MessageSerializer serializer = MessageSerializer.select("not/real", MessageSerializer.DEFAULT_RESULT_SERIALIZER);
        assertNotNull(serializer);
        assertTrue(ToStringMessageSerializer.class.isAssignableFrom(serializer.getClass()));
    }

    @Test
    public void serializeToStringNull() throws Exception {
        final String results = MessageSerializer.select("text/plain", MessageSerializer.DEFAULT_RESULT_SERIALIZER).serializeResult(null, new Context(msg, null, null, null, null));
        assertEquals("2d62161b-9544-4f39-af44-62ec49f9a595>>null", results);
    }

    @Test
    public void serializeToStringAVertex() throws Exception {
        final TinkerGraph g = TinkerFactory.createClassic();
        final Vertex v = g.query().has("name", Compare.EQUAL, "marko").vertices().iterator().next();
        final String results = MessageSerializer.select("text/plain", MessageSerializer.DEFAULT_RESULT_SERIALIZER).serializeResult(v, new Context(msg, null, null, null, null));
        assertEquals("2d62161b-9544-4f39-af44-62ec49f9a595>>v[1]", results);
    }


}
