package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

/**
 * Implementation of the {@code DefaultSerializerProvider} for Jackson that users the {@code ToStringSerializer} for
 * unknown types.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GraphSONSerializerProvider extends DefaultSerializerProvider {
    private static final long serialVersionUID = 1L;
    private static final ToStringSerializer toStringSerializer = new ToStringSerializer();

    public GraphSONSerializerProvider() {
        super();
    }

    protected GraphSONSerializerProvider(final SerializerProvider src,
                                         final SerializationConfig config, final SerializerFactory f) {
        super(src, config, f);
    }

    @Override
    public JsonSerializer<Object> getUnknownTypeSerializer(final Class<?> aClass) {
        return toStringSerializer;
    }

    @Override
    public GraphSONSerializerProvider createInstance(final SerializationConfig config,
                                                     final SerializerFactory jsf) {
        return new GraphSONSerializerProvider(this, config, jsf);
    }
}
