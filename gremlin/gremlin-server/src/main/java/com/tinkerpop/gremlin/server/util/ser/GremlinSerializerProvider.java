package com.tinkerpop.gremlin.server.util.ser;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

/**
 * Implementation of the DefaultSerializerProvider for Jackson that users the ToStringSerializer for unknown types.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinSerializerProvider extends DefaultSerializerProvider {
    private static final long serialVersionUID = 1L;
    private static final ToStringSerializer toStringSerializer = new ToStringSerializer();

    public GremlinSerializerProvider() {
        super();
    }

    protected GremlinSerializerProvider(final SerializerProvider src,
                                        final SerializationConfig config, final SerializerFactory f) {
        super(src, config, f);
    }

    @Override
    public JsonSerializer<Object> getUnknownTypeSerializer(final Class<?> aClass) {
        return toStringSerializer;
    }

    @Override
    public GremlinSerializerProvider createInstance(final SerializationConfig config,
                                                    final SerializerFactory jsf) {
        return new GremlinSerializerProvider(this, config, jsf);
    }
}