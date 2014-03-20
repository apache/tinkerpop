package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;

import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONObjectMapper extends ObjectMapper {
    public GraphSONObjectMapper() {
        this(null, false);
    }

    public GraphSONObjectMapper(final SimpleModule custom) {
        this(custom, false);
    }

    public GraphSONObjectMapper(final SimpleModule custom, final boolean loadExternalModules) {
        disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        // this provider toStrings all unknown classes and converts keys in Map objects that are Object to String.
        final DefaultSerializerProvider provider = new GraphSONSerializerProvider();
        provider.setDefaultKeySerializer(new GraphSONModule.GraphSONKeySerializer());
        setSerializerProvider(provider);

        registerModule(new GraphSONModule());
        Optional.ofNullable(custom).ifPresent(this::registerModule);

        // plugin external serialization modules
        if (loadExternalModules)
            findAndRegisterModules();
    }
}
