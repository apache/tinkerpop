package com.tinkerpop.gremlin.server.util.ser;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.tinkergraph.TinkerFactory;
import com.tinkerpop.tinkergraph.TinkerGraph;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.MessageSerializer;
import com.tinkerpop.gremlin.server.RequestMessage;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ToStringResultSerializerTest {

    private static final MessageSerializer serializer = new ToStringMessageSerializer();
    private static final RequestMessage msg = new RequestMessage.Builder("op")
            .overrideRequestId(UUID.fromString("2D62161B-9544-4F39-AF44-62EC49F9A595")).build();

    @Test
    public void serializeToStringNull() throws Exception {
        final String results = serializer.serializeResult(null, new Context(msg, null, null, null, null));
        assertEquals("2d62161b-9544-4f39-af44-62ec49f9a595>>null", results);
    }

    @Test
    public void serializeToStringAVertex() throws Exception {
        final TinkerGraph g = TinkerFactory.createClassic();
        final Vertex v = g.query().has("name", Compare.EQUAL, "marko").vertices().iterator().next();
        final String results = serializer.serializeResult(v, new Context(msg, null, null, null, null));
        assertEquals("2d62161b-9544-4f39-af44-62ec49f9a595>>v[1]", results);
    }
}
