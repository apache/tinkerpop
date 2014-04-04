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

    private GraphSONObjectMapper(final SimpleModule custom, final boolean loadCustomSerializers, final boolean normalize) {
        disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        // this provider toStrings all unknown classes and converts keys in Map objects that are Object to String.
        final DefaultSerializerProvider provider = new GraphSONSerializerProvider();
        provider.setDefaultKeySerializer(new GraphSONModule.GraphSONKeySerializer());
        setSerializerProvider(provider);

        registerModule(new GraphSONModule(normalize));
        Optional.ofNullable(custom).ifPresent(this::registerModule);

        // plugin external serialization modules
        if (loadCustomSerializers)
            findAndRegisterModules();
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private SimpleModule custom = null;
        private boolean loadCustomSerializers = false;
        private boolean normalize = false;

        private Builder() {}

        public Builder customSerializer(final SimpleModule custom) {
            this.custom = custom;
            return this;
        }

        public Builder loadCustomSerializers(final boolean loadCustomSerializers) {
            this.loadCustomSerializers = loadCustomSerializers;
            return this;
        }

        public Builder normalize(final boolean normalize) {
            this.normalize = normalize;
            return this;
        }

        public GraphSONObjectMapper build() {
            return new GraphSONObjectMapper(custom, loadCustomSerializers, normalize);
        }
    }
}
