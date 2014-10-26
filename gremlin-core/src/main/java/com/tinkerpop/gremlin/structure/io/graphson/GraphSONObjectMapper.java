package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;

/**
 * An extension to the standard Jackson {@code ObjectMapper} which automatically registers the standard
 * {@link GraphSONModule} for serializing {@link com.tinkerpop.gremlin.structure.Graph} elements.  This class
 * can be used for generalized JSON serialization tasks that require meeting GraphSON standards.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONObjectMapper extends ObjectMapper {

    private GraphSONObjectMapper(final SimpleModule custom, final boolean loadCustomSerializers,
                                 final boolean normalize, final boolean embedTypes) {
        disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        if (embedTypes)
            enableDefaultTypingAsProperty(DefaultTyping.NON_FINAL, GraphSONTokens.CLASS);

        if (normalize)
            enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

        // this provider toStrings all unknown classes and converts keys in Map objects that are Object to String.
        final DefaultSerializerProvider provider = new GraphSONSerializerProvider();
        provider.setDefaultKeySerializer(new GraphSONModule.GraphSONKeySerializer());
        setSerializerProvider(provider);

        registerModule(new GraphSONModule(normalize));
        if (custom != null) this.registerModule(custom);

        // plugin external serialization modules
        if (loadCustomSerializers)
            findAndRegisterModules();

        // keep streams open to accept multiple values (e.g. multiple vertices)
        _jsonFactory.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private SimpleModule custom = null;
        private boolean loadCustomModules = false;
        private boolean normalize = false;
        private boolean embedTypes = false;

        private Builder() {
        }

        /**
         * Supply a custom module for serialization/deserialization.
         */
        public Builder customModule(final SimpleModule custom) {
            this.custom = custom;
            return this;
        }

        /**
         * Try to load {@code SimpleModule} instances from the current classpath.  These are loaded in addition to
         * the one supplied to the {@link #customModule(com.fasterxml.jackson.databind.module.SimpleModule)};
         */
        public Builder loadCustomModules(final boolean loadCustomModules) {
            this.loadCustomModules = loadCustomModules;
            return this;
        }

        /**
         * Forces keys to be sorted.
         */
        public Builder normalize(final boolean normalize) {
            this.normalize = normalize;
            return this;
        }

        /**
         * Embeds Java types into generated JSON to clarify their origins.
         */
        public Builder embedTypes(final boolean embedTypes) {
            this.embedTypes = embedTypes;
            return this;
        }

        public GraphSONObjectMapper create() {
            return new GraphSONObjectMapper(custom, loadCustomModules, normalize, embedTypes);
        }
    }
}
