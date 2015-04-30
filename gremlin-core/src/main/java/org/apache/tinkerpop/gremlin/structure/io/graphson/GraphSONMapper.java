/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * An extension to the standard Jackson {@code ObjectMapper} which automatically registers the standard
 * {@link GraphSONModule} for serializing {@link Graph} elements.  This class
 * can be used for generalized JSON serialization tasks that require meeting GraphSON standards.
 * <p/>
 * {@link Graph} implementations providing an {@link IoRegistry} should register their {@code SimpleModule}
 * implementations to it as follows:
 * <pre>
 * {@code
 * IoRegistry registry = new IoRegistry();
 * registry.register(GraphSONIo.class, null, new MySimpleModule());
 * }
 * </pre>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONMapper implements Mapper<ObjectMapper> {

    private final List<SimpleModule> customModules;
    private final boolean loadCustomSerializers;
    private final boolean normalize;
    private final boolean embedTypes;

    private GraphSONMapper(final List<SimpleModule> customModules, final boolean loadCustomSerializers,
                           final boolean normalize, final boolean embedTypes) {
        this.customModules = customModules;
        this.loadCustomSerializers = loadCustomSerializers;
        this.normalize = normalize;
        this.embedTypes = embedTypes;
    }

    @Override
    public ObjectMapper createMapper() {
        final ObjectMapper om = new ObjectMapper();
        om.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        if (embedTypes)
            om.enableDefaultTypingAsProperty(ObjectMapper.DefaultTyping.NON_FINAL, GraphSONTokens.CLASS);

        if (normalize)
            om.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

        // this provider toStrings all unknown classes and converts keys in Map objects that are Object to String.
        final DefaultSerializerProvider provider = new GraphSONSerializerProvider();
        provider.setDefaultKeySerializer(new GraphSONSerializers.GraphSONKeySerializer());
        om.setSerializerProvider(provider);

        om.registerModule(new GraphSONModule(normalize));
        customModules.forEach(om::registerModule);

        // plugin external serialization modules
        if (loadCustomSerializers)
            om.findAndRegisterModules();

        // keep streams open to accept multiple values (e.g. multiple vertices)
        om.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

        return om;
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder implements Mapper.Builder<Builder> {
        private List<SimpleModule> customModules = new ArrayList<>();
        private boolean loadCustomModules = false;
        private boolean normalize = false;
        private boolean embedTypes = false;
        private IoRegistry registry = null;

        private Builder() {
        }

        @Override
        public Builder addRegistry(final IoRegistry registry) {
            this.registry = registry;
            return this;
        }

        /**
         * Supply a mapper module for serialization/deserialization.
         */
        public Builder addCustomModule(final SimpleModule custom) {
            this.customModules.add(custom);
            return this;
        }

        /**
         * Try to load {@code SimpleModule} instances from the current classpath.  These are loaded in addition to
         * the one supplied to the {@link #addCustomModule(com.fasterxml.jackson.databind.module.SimpleModule)};
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

        public GraphSONMapper create() {
            if (registry != null) {
                final List<Pair<Class, SimpleModule>> simpleModules = registry.find(GraphSONIo.class, SimpleModule.class);
                simpleModules.stream().map(Pair::getValue1).forEach(this.customModules::add);
            }

            return new GraphSONMapper(customModules, loadCustomModules, normalize, embedTypes);
        }
    }
}
