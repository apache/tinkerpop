package com.tinkerpop.gremlin.driver.ser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONObjectMapper;

import java.nio.ByteBuffer;

/**
 * Serialize results to JSON with version 1.0.x schema.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JsonMessageSerializerV1d0 extends AbstractJsonMessageSerializerV1d0 {
    private static final String MIME_TYPE = "application/json";

    /**
     * ObjectMapper instance for JSON serialization via Jackson databind.  Uses custom serializers to write
     * out {@link com.tinkerpop.gremlin.structure.Graph} objects and {@code toString} for unknown objects.
     */
    protected static final ObjectMapper mapper = GraphSONObjectMapper.create()
            .customModule(new GremlinServerModule())
            .embedTypes(false)
            .build();

    private static byte[] header;

    static {
        final ByteBuffer buffer = ByteBuffer.allocate(MIME_TYPE.length() + 1);
        buffer.put((byte) MIME_TYPE.length());
        buffer.put(MIME_TYPE.getBytes());
        header = buffer.array();
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{MIME_TYPE};
    }

    @Override
    ObjectMapper obtainMapper() {
        return mapper;
    }

    @Override
    byte[] obtainHeader() {
        return header;
    }
}
