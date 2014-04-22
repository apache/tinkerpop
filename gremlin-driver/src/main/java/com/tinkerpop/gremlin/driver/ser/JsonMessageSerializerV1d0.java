package com.tinkerpop.gremlin.driver.ser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONObjectMapper;

/**
 * Serialize results to JSON with version 1.0.x schema.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JsonMessageSerializerV1d0 extends AbstractJsonMessageSerializerV1d0 {
    /**
     * ObjectMapper instance for JSON serialization via Jackson databind.  Uses custom serializers to write
     * out {@link com.tinkerpop.gremlin.structure.Graph} objects and {@code toString} for unknown objects.
     */
    protected static final ObjectMapper mapper = GraphSONObjectMapper.create()
            .customModule(new GremlinServerModule())
            .embedTypes(false)
            .build();

    @Override
    public String[] mimeTypesSupported() {
        return new String[]{"application/json"};
    }

    @Override
    ObjectMapper obtainMapper() {
        return mapper;
    }
}
