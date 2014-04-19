package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.ser.JsonMessageSerializerV1d0;
import com.tinkerpop.gremlin.driver.ser.ToStringMessageSerializer;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MessageSerializerTest {

    private static final RequestMessage msg = RequestMessage.create("op")
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

}
