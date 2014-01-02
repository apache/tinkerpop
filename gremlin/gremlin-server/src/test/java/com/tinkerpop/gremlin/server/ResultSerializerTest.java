package com.tinkerpop.gremlin.server;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import com.tinkerpop.blueprints.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.server.util.ser.JsonResultSerializerV1d0;
import com.tinkerpop.gremlin.server.util.ser.ToStringResultSerializer;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultSerializerTest {

    private static final RequestMessage msg = new RequestMessage.Builder("op")
            .overrideRequestId(UUID.fromString("2D62161B-9544-4F39-AF44-62EC49F9A595")).build();

    @Test
    public void shouldGetTextSerializer() {
        final ResultSerializer serializer = ResultSerializer.select("text/plain");
        assertNotNull(serializer);
        assertTrue(ToStringResultSerializer.class.isAssignableFrom(serializer.getClass()));
    }

    @Test
    public void shouldGetJsonSerializer() {
        final ResultSerializer serializerGremlinV1 = ResultSerializer.select("application/vnd.gremlin-v1.0+json");
        assertNotNull(serializerGremlinV1);
        assertTrue(JsonResultSerializerV1d0.class.isAssignableFrom(serializerGremlinV1.getClass()));

        final ResultSerializer serializerJson = ResultSerializer.select("application/json");
        assertNotNull(serializerJson);
        assertTrue(JsonResultSerializerV1d0.class.isAssignableFrom(serializerJson.getClass()));
    }

    @Test
    public void shouldGetTextSerializerAsDefault() {
        final ResultSerializer serializer = ResultSerializer.select("not/real");
        assertNotNull(serializer);
        assertTrue(ToStringResultSerializer.class.isAssignableFrom(serializer.getClass()));
    }

    @Test
    public void serializeToStringNull() throws Exception {
        final String results = ResultSerializer.select("text/plain").serialize(null, new Context(msg, null, null, null, null));
        assertEquals("2d62161b-9544-4f39-af44-62ec49f9a595>>null", results);
    }

    @Test
    public void serializeToStringAVertex() throws Exception {
        final TinkerGraph g = TinkerFactory.createClassic();
        final Vertex v = g.query().has("name", Compare.EQUAL, "marko").vertices().iterator().next();
        final String results = ResultSerializer.select("text/plain").serialize(v, new Context(msg, null, null, null, null));
        assertEquals("2d62161b-9544-4f39-af44-62ec49f9a595>>v[1]", results);
    }


}
