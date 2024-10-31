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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonTypeInfo;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.StreamReadConstraints;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationFeature;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeResolverBuilder;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.DefaultSerializerProvider;
import org.javatuples.Pair;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/**
 * An extension to the standard Jackson {@code ObjectMapper} which automatically registers the standard
 * {@link GraphSONModule} for serializing {@link Graph} elements.  This class
 * can be used for generalized JSON serialization tasks that require meeting GraphSON standards.
 * <p/>
 * {@link Graph} implementations providing an {@link IoRegistry} should register their {@code SimpleModule}
 * implementations to it as follows:
 * <pre>
 * {@code
 * public class MyGraphIoRegistry extends AbstractIoRegistry {
 *   public MyGraphIoRegistry() {
 *     register(GraphSONIo.class, null, new MyGraphSimpleModule());
 *   }
 * }
 * }
 * </pre>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONMapper implements Mapper<ObjectMapper> {
    public static final int DEFAULT_MAX_NUMBER_LENGTH = 10000;

    private final List<SimpleModule> customModules;
    private final boolean loadCustomSerializers;
    private final boolean normalize;
    private final GraphSONVersion version;
    private final TypeInfo typeInfo;
    private final StreamReadConstraints streamReadConstraints;

    private GraphSONMapper(final Builder builder) {
        this.customModules = builder.customModules;
        this.loadCustomSerializers = builder.loadCustomModules;
        this.normalize = builder.normalize;
        this.version = builder.version;
        this.streamReadConstraints = builder.streamReadConstraintsBuilder.build();
        this.typeInfo = builder.typeInfo;
    }

    @Override
    public ObjectMapper createMapper() {
        final ObjectMapper om = new ObjectMapper(JsonFactory.builder().streamReadConstraints(streamReadConstraints).build());
        if (version != GraphSONVersion.V4_0) {
            om.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        }

        final GraphSONModule graphSONModule = version.getBuilder().create(normalize, typeInfo);
        om.registerModule(graphSONModule);
        customModules.forEach(om::registerModule);

        // plugin external serialization modules
        if (loadCustomSerializers)
            om.findAndRegisterModules();

        if ((version == GraphSONVersion.V4_0 || version == GraphSONVersion.V3_0 || version == GraphSONVersion.V2_0) &&
                typeInfo != TypeInfo.NO_TYPES) {
            final GraphSONTypeIdResolver graphSONTypeIdResolver = new GraphSONTypeIdResolver();
            final TypeResolverBuilder typer = new GraphSONTypeResolverBuilder(version)
                    .typesEmbedding(this.typeInfo)
                    .valuePropertyName(GraphSONTokens.VALUEPROP)
                    .init(JsonTypeInfo.Id.CUSTOM, graphSONTypeIdResolver)
                    .typeProperty(GraphSONTokens.VALUETYPE);

            // Registers native Java types that are supported by Jackson
            registerJavaBaseTypes(graphSONTypeIdResolver);

            // Registers the GraphSON Module's types
            graphSONModule.getTypeDefinitions().forEach(
                    (targetClass, typeId) -> graphSONTypeIdResolver.addCustomType(
                            String.format("%s:%s", graphSONModule.getTypeNamespace(), typeId), targetClass));

            // Register types to typeResolver for the Custom modules
            customModules.forEach(e -> {
                if (e instanceof TinkerPopJacksonModule) {
                    final TinkerPopJacksonModule mod = (TinkerPopJacksonModule) e;
                    final Map<Class, String> moduleTypeDefinitions = mod.getTypeDefinitions();
                    if (moduleTypeDefinitions != null) {
                        if (mod.getTypeNamespace() == null || mod.getTypeNamespace().isEmpty())
                            throw new IllegalStateException("Cannot specify a module for GraphSON 2.0 with type definitions but without a type Domain. " +
                                    "If no specific type domain is required, use Gremlin's default domain, \"gremlin\" but there may be collisions.");

                        moduleTypeDefinitions.forEach((targetClass, typeId) -> graphSONTypeIdResolver.addCustomType(
                                        String.format("%s:%s", mod.getTypeNamespace(), typeId), targetClass));
                    }
                }
            });
            om.setDefaultTyping(typer);
        } else if (version == GraphSONVersion.V1_0 || version == GraphSONVersion.V2_0) {
            if (typeInfo == TypeInfo.PARTIAL_TYPES) {
                final TypeResolverBuilder<?> typer = new StdTypeResolverBuilder()
                        .init(JsonTypeInfo.Id.CLASS, null)
                        .inclusion(JsonTypeInfo.As.PROPERTY)
                        .typeProperty(GraphSONTokens.CLASS);
                om.setDefaultTyping(typer);
            }
        } else if (version == GraphSONVersion.V3_0 || version == GraphSONVersion.V4_0) {

        } else {
            throw new IllegalStateException("Unknown GraphSONVersion: " + version);
        }

        // Starting with GraphSONv4, only types that can be returned from the result of a traversal are supported. This
        // differs to previous versions where a gremlin-groovy script could return any type. So if an unknown type is
        // encountered, an error should be thrown.
        if (version != GraphSONVersion.V4_0) {
            // this provider toStrings all unknown classes and converts keys in Map objects that are Object to String.
            final DefaultSerializerProvider provider = new GraphSONSerializerProvider(version);
            om.setSerializerProvider(provider);
        }

        if (normalize)
            om.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

        // keep streams open to accept multiple values (e.g. multiple vertices)
        om.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        return om;
    }

    public GraphSONVersion getVersion() {
        return this.version;
    }

    public static Builder build() {
        return new Builder();
    }

    /**
     * Create a new Builder from a given {@link GraphSONMapper}.
     *
     * @return a new builder, with properties taken from the original mapper already applied.
     */
    public static Builder build(final GraphSONMapper mapper) {
        Builder builder = build();

        builder.customModules = mapper.customModules;
        builder.version = mapper.version;
        builder.loadCustomModules = mapper.loadCustomSerializers;
        builder.normalize = mapper.normalize;
        builder.typeInfo = mapper.typeInfo;
        builder.streamReadConstraintsBuilder = mapper.streamReadConstraints.rebuild();

        return builder;
    }

    public TypeInfo getTypeInfo() {
        return this.typeInfo;
    }

    private void registerJavaBaseTypes(final GraphSONTypeIdResolver graphSONTypeIdResolver) {
        Arrays.asList(
                UUID.class,
                Class.class,
                Calendar.class,
                Date.class,
                TimeZone.class,
                Timestamp.class
        ).forEach(e -> graphSONTypeIdResolver.addCustomType(String.format("%s:%s", GraphSONTokens.GREMLIN_TYPE_NAMESPACE, e.getSimpleName()), e));
    }

    public static class Builder implements Mapper.Builder<Builder> {
        private List<SimpleModule> customModules = new ArrayList<>();
        private List<GraphSONModule.GraphSONModuleBuilder> customModuleBuilders = new ArrayList<>();
        private boolean loadCustomModules = false;
        private boolean normalize = false;
        private List<IoRegistry> registries = new ArrayList<>();
        private GraphSONVersion version = GraphSONVersion.V4_0;
        private boolean includeDefaultXModule = false;
        private StreamReadConstraints.Builder streamReadConstraintsBuilder = StreamReadConstraints.builder()
                .maxNumberLength(DEFAULT_MAX_NUMBER_LENGTH);
        private TypeInfo typeInfo = null;

        private Builder() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder addRegistry(final IoRegistry registry) {
            registries.add(registry);
            return this;
        }

        /**
         * Set the version of GraphSON to use. The default is {@link GraphSONVersion#V4_0}.
         */
        public Builder version(final GraphSONVersion version) {
            this.version = version;
            return this;
        }

        /**
         * Set the version of GraphSON to use.
         */
        public Builder version(final String version) {
            this.version = GraphSONVersion.valueOf(version);
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
         * Supplies a mapper module builder to be lazily constructed. The advantage to using this mechanism over
         * {@link #addCustomModule(SimpleModule)} is that if the module is constructed with {@link TypeInfo} it can
         * inherit it from the value supplied to {@link #typeInfo(TypeInfo)} (as well as the {@link #normalize(boolean)}
         * option.
         */
        public Builder addCustomModule(final GraphSONModule.GraphSONModuleBuilder moduleBuilder) {
            this.customModuleBuilders.add(moduleBuilder);
            return this;
        }

        /**
         * Supply a default extension module of V2_0, V3_0 and V4_0 for serialization/deserialization.
         */
        public Builder addDefaultXModule(final boolean includeDefaultXModule) {
            this.includeDefaultXModule = includeDefaultXModule;
            return this;
        }

        /**
         * Try to load {@code SimpleModule} instances from the current classpath.  These are loaded in addition to
         * the one supplied to the {@link #addCustomModule(SimpleModule)};
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
         * Specify if the values are going to be typed or not, and at which level.
         *
         * The level can be {@link TypeInfo#NO_TYPES} or {@link TypeInfo#PARTIAL_TYPES}, and could be extended in the
         * future.
         */
        public Builder typeInfo(final TypeInfo typeInfo) {
            this.typeInfo = typeInfo;
            return this;
        }

        public Builder maxNumberLength(final int maxNumLength) {
            this.streamReadConstraintsBuilder.maxNumberLength(maxNumLength);
            return this;
        }

        public Builder maxNestingDepth(final int maxNestingDepth) {
            this.streamReadConstraintsBuilder.maxNestingDepth(maxNestingDepth);
            return this;
        }

        public Builder maxStringLength(final int maxStringLength) {
            this.streamReadConstraintsBuilder.maxStringLength(maxStringLength);
            return this;
        }

        public GraphSONMapper create() {
            registries.forEach(registry -> {
                final List<Pair<Class, SimpleModule>> simpleModules = registry.find(GraphSONIo.class, SimpleModule.class);
                simpleModules.stream().map(Pair::getValue1).forEach(this.customModules::add);
            });

            typeInfo = inferTypeInfo(typeInfo, version);

            // finish building off the modules.
            customModuleBuilders.forEach(b -> {
                this.addCustomModule(b.create(this.normalize, typeInfo));
            });

            if (includeDefaultXModule) {
                if (this.version == GraphSONVersion.V2_0) {
                    this.addCustomModule(GraphSONXModuleV2.build().create(this.normalize, typeInfo));
                } else if (this.version == GraphSONVersion.V3_0) {
                    this.addCustomModule(GraphSONXModuleV3.build().create(this.normalize, typeInfo));
                } else if (this.version == GraphSONVersion.V4_0) {
                    this.addCustomModule(GraphSONXModuleV4.build().create(this.normalize, typeInfo));
                }
            }

            return new GraphSONMapper(this);
        }

        /**
         * User the version to infer the {@link TypeInfo} if it is not explicitly supplied. GraphSON 1.0 defaults to
         * no types, since it's Jackson type system is fairly impenetrable, but we otherwise use types.
         */
        private static TypeInfo inferTypeInfo(final TypeInfo typeInfo, final GraphSONVersion version) {
            if (null == typeInfo)
                return version == GraphSONVersion.V1_0 ? TypeInfo.NO_TYPES : TypeInfo.PARTIAL_TYPES;
            else
                return typeInfo;
        }
    }
}
