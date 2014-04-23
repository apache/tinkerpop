package com.tinkerpop.gremlin.driver;

import com.tinkerpop.gremlin.driver.ser.JsonMessageSerializerGremlinV1d0;
import com.tinkerpop.gremlin.driver.ser.JsonMessageSerializerV1d0;
import com.tinkerpop.gremlin.driver.ser.Serializers;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MessageSerializerTest {

    @Test
    public void shouldGetJsonSerializer() {
        final MessageSerializer serializerGremlinV1 = MessageSerializer.select(Serializers.JSON_V1D0.getValue(), MessageSerializer.DEFAULT_RESULT_SERIALIZER);
        assertNotNull(serializerGremlinV1);
        assertTrue(JsonMessageSerializerGremlinV1d0.class.isAssignableFrom(serializerGremlinV1.getClass()));

        final MessageSerializer serializerJson = MessageSerializer.select(Serializers.JSON.getValue(), MessageSerializer.DEFAULT_RESULT_SERIALIZER);
        assertNotNull(serializerJson);
        assertTrue(JsonMessageSerializerV1d0.class.isAssignableFrom(serializerJson.getClass()));
    }

    @Test
    public void shouldGetTextSerializerAsDefault() {
        final MessageSerializer serializer = MessageSerializer.select("not/real", MessageSerializer.DEFAULT_RESULT_SERIALIZER);
        assertNotNull(serializer);
        assertTrue(JsonMessageSerializerV1d0.class.isAssignableFrom(serializer.getClass()));
    }

}
