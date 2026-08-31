/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.structure.io.pdt.PDTRegistry;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonTypeInfo;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.StreamReadConstraints;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationFeature;
import org.apache.tinkerpop.shaded.jackson.databind.JavaType;
import org.apache.tinkerpop.shaded.jackson.databind.DatabindContext;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.cfg.MapperConfig;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.PolymorphicTypeValidator;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeIdResolver;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.NamedType;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeResolverBuilder;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.DefaultSerializerProvider;
import org.javatuples.Pair;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
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

    // Java base value types registered for GraphSON 2.0/3.0, shared by registerJavaBaseTypes and the V1 name set.
    private static final List<Class> GRAPHSON_JAVA_BASE_TYPES = Arrays.asList(
            UUID.class, Class.class, Calendar.class, Date.class, TimeZone.class, Timestamp.class);

    // concrete runtime class names GraphSON 1.0 writes that registeredV2V3Types() does not supply, as it declares
    // only the java.util.List, Map and Set interfaces
    private static final List<String> GRAPHSON_1_0_ALLOWED_EXTRA_TYPE_NAMES = Arrays.asList(
            "java.lang.Boolean",
            "java.lang.Object",
            "java.lang.String",
            "java.net.URI",
            "java.sql.Date",
            "java.sql.Time",
            "java.time.DayOfWeek",
            "java.time.Month",
            "java.util.ArrayDeque",
            "java.util.ArrayList",
            "java.util.Arrays$ArrayList",
            "java.util.Collections$EmptyList",
            "java.util.Collections$EmptyMap",
            "java.util.Collections$EmptySet",
            "java.util.Collections$SingletonList",
            "java.util.Collections$SingletonMap",
            "java.util.Collections$SingletonSet",
            "java.util.Collections$UnmodifiableList",
            "java.util.Collections$UnmodifiableMap",
            "java.util.Collections$UnmodifiableRandomAccessList",
            "java.util.Collections$UnmodifiableSet",
            "java.util.Currency",
            "java.util.GregorianCalendar",
            "java.util.HashMap",
            "java.util.HashSet",
            "java.util.LinkedHashMap",
            "java.util.LinkedHashSet",
            "java.util.LinkedList",
            "java.util.Locale",
            "java.util.Properties",
            "java.util.TreeMap",
            "java.util.TreeSet");
    private static final Set<String> GRAPHSON_1_0_ALLOWED_TYPE_NAMES = graphSON1dAllowedTypeNames();
    public static final int DEFAULT_MAX_NUMBER_LENGTH = 10000;

    private final List<SimpleModule> customModules;
    private final boolean loadCustomSerializers;
    private final boolean normalize;
    private final GraphSONVersion version;
    private final TypeInfo typeInfo;
    private final StreamReadConstraints streamReadConstraints;
    private final PDTRegistry pdtRegistry;
    final List<String> allowedTypeIdNames;

    private GraphSONMapper(final Builder builder) {
        this.customModules = builder.customModules;
        this.loadCustomSerializers = builder.loadCustomModules;
        this.normalize = builder.normalize;
        this.version = builder.version;
        this.streamReadConstraints = builder.streamReadConstraintsBuilder.build();
        this.typeInfo = builder.typeInfo;
        this.pdtRegistry = builder.pdtRegistry;
        this.allowedTypeIdNames = builder.allowedTypeIdNames;
    }

    @Override
    public ObjectMapper createMapper() {
        final ObjectMapper om = new ObjectMapper(JsonFactory.builder().streamReadConstraints(streamReadConstraints).build());
        if (version != GraphSONVersion.V4_0) {
            om.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        }

        final GraphSONModule graphSONModule = version.getBuilder().create(normalize, typeInfo);
        if (pdtRegistry != null && graphSONModule instanceof GraphSONModule.GraphSONModuleV4) {
            ((GraphSONModule.GraphSONModuleV4) graphSONModule).setPdtRegistry(pdtRegistry);
        }
        om.registerModule(graphSONModule);
        customModules.forEach(om::registerModule);

        // plugin external serialization modules
        if (loadCustomSerializers)
            om.findAndRegisterModules();

        if ((version == GraphSONVersion.V4_0 || version == GraphSONVersion.V3_0 || version == GraphSONVersion.V2_0) &&
                typeInfo != TypeInfo.NO_TYPES) {
            final GraphSONTypeIdResolver graphSONTypeIdResolver = new GraphSONTypeIdResolver();
            if (pdtRegistry != null && version == GraphSONVersion.V4_0) {
                graphSONTypeIdResolver.setPdtRegistry(pdtRegistry);
            }
            final TypeResolverBuilder typer = new GraphSONTypeResolverBuilder(version)
                    .typesEmbedding(this.typeInfo)
                    .valuePropertyName(GraphSONTokens.VALUEPROP)
                    .init(JsonTypeInfo.Id.CUSTOM, graphSONTypeIdResolver)
                    .typeProperty(GraphSONTokens.VALUETYPE);

            // Registers native Java types that are supported by Jackson
            if (version == GraphSONVersion.V4_0) {
                graphSONTypeIdResolver.addCustomType(
                        String.format("%s:%s", GraphSONTokens.GREMLIN_TYPE_NAMESPACE, UUID.class.getSimpleName()), UUID.class);
            } else {
                registerJavaBaseTypes(graphSONTypeIdResolver);
            }

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
                final Set<String> allowedNames = new HashSet<>(GRAPHSON_1_0_ALLOWED_TYPE_NAMES);
                allowedNames.addAll(allowedTypeIdNames);
                final PolymorphicTypeValidator typeValidator = graphSON1dTypeValidator(allowedNames);
                final TypeResolverBuilder<?> typer = new StdTypeResolverBuilder() {
                    @Override
                    public PolymorphicTypeValidator subTypeValidator(final MapperConfig<?> config) {
                        return typeValidator;
                    }

                    @Override
                    protected TypeIdResolver idResolver(final MapperConfig<?> config, final JavaType baseType,
                                                        final PolymorphicTypeValidator subtypeValidator,
                                                        final Collection<NamedType> subtypes,
                                                        final boolean forSer, final boolean forDeser) {
                        return new GraphSON1dScreeningIdResolver(
                                super.idResolver(config, baseType, subtypeValidator, subtypes, forSer, forDeser));
                    }
                }.init(JsonTypeInfo.Id.CLASS, null)
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
        } else if (pdtRegistry != null) {
            // For V4 with a pdtRegistry, set a provider that converts adapter-registered types to PDT
            final DefaultSerializerProvider provider = new PdtGraphSONSerializerProviderV4(pdtRegistry);
            om.setSerializerProvider(provider);
        }

        if (normalize)
            om.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

        // keep streams open to accept multiple values (e.g. multiple vertices)
        om.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        return om;
    }

    /**
     * A {@link PolymorphicTypeValidator} for GraphSON 1.0 embedded types that decides a simple type id from its name
     * alone. Parameterized type ids are handled by {@link GraphSON1dScreeningIdResolver}, as the validator is not
     * shown a type id's arguments.
     */
    private static PolymorphicTypeValidator graphSON1dTypeValidator(final Set<String> allowedNames) {
        return new PolymorphicTypeValidator.Base() {
            @Override
            public Validity validateBaseType(final MapperConfig<?> config, final JavaType baseType) {
                // must stay INDETERMINATE: on ALLOWED, Jackson substitutes LaissezFaireSubTypeValidator and the
                // name set below is no longer consulted
                return Validity.INDETERMINATE;
            }

            @Override
            public Validity validateSubClassName(final MapperConfig<?> config, final JavaType baseType,
                                                 final String subClassName) {
                return isAllowedTypeName(subClassName, allowedNames) ? Validity.ALLOWED : Validity.DENIED;
            }

            @Override
            public Validity validateSubType(final MapperConfig<?> config, final JavaType baseType,
                                            final JavaType subType) {
                // reached only when validateSubClassName returns INDETERMINATE, which it never does, so this is not
                // where a name is decided today
                return isAllowedTypeName(subType.getRawClass().getName(), allowedNames)
                        ? Validity.ALLOWED : Validity.DENIED;
            }
        };
    }

    /**
     * Wraps the class-name {@link TypeIdResolver} to refuse a parameterized GraphSON 1.0 type id, meaning one
     * containing '{@code <}', rather than hand it to the delegate.
     */
    private static final class GraphSON1dScreeningIdResolver implements TypeIdResolver {
        private final TypeIdResolver delegate;
        private JavaType baseType;

        private GraphSON1dScreeningIdResolver(final TypeIdResolver delegate) {
            this.delegate = delegate;
        }

        @Override
        public void init(final JavaType baseType) {
            this.baseType = baseType;
            delegate.init(baseType);
        }

        @Override
        public JavaType typeFromId(final DatabindContext context, final String id) throws IOException {
            if (id.indexOf('<') >= 0) {
                if (context instanceof DeserializationContext)
                    throw ((DeserializationContext) context).invalidTypeIdException(baseType, id,
                            "GraphSON 1.0 does not permit a parameterized type id");
                throw new IOException("GraphSON 1.0 does not permit a parameterized type id: " + id);
            }
            return delegate.typeFromId(context, id);
        }

        @Override
        public String idFromValue(final Object value) {
            return delegate.idFromValue(value);
        }

        @Override
        public String idFromValueAndType(final Object value, final Class<?> suggestedType) {
            return delegate.idFromValueAndType(value, suggestedType);
        }

        @Override
        public String idFromBaseType() {
            return delegate.idFromBaseType();
        }

        @Override
        public String getDescForKnownTypeIds() {
            return delegate.getDescForKnownTypeIds();
        }

        @Override
        public JsonTypeInfo.Id getMechanism() {
            return delegate.getMechanism();
        }
    }

    // TypeInfo does not affect the (static) type-definition map, so NO_TYPES is passed
    private static Set<Class> registeredV2V3Types() {
        final Set<Class> registered = new LinkedHashSet<>(GRAPHSON_JAVA_BASE_TYPES);
        registered.addAll(GraphSONVersion.V2_0.getBuilder().create(false, TypeInfo.NO_TYPES)
                .getTypeDefinitions().keySet());
        registered.addAll(GraphSONVersion.V3_0.getBuilder().create(false, TypeInfo.NO_TYPES)
                .getTypeDefinitions().keySet());
        registered.addAll(GraphSONXModuleV2.build().create(false, TypeInfo.NO_TYPES)
                .getTypeDefinitions().keySet());
        registered.addAll(GraphSONXModuleV3.build().create(false, TypeInfo.NO_TYPES)
                .getTypeDefinitions().keySet());
        return registered;
    }

    /**
     * The exact class names GraphSON 1.0 embedded-type deserialization resolves by default.
     */
    private static Set<String> graphSON1dAllowedTypeNames() {
        final Set<String> names = new HashSet<>(GRAPHSON_1_0_ALLOWED_EXTRA_TYPE_NAMES);
        names.addAll(graphSON1dDerivedTypeNames());
        return names;
    }

    /**
     * The GraphSON 1.0 allowed names derived from {@link #registeredV2V3Types()}. Package private so a test can pin
     * it, since a new type definition in any GraphSON 2.0/3.0 module widens this set as a side effect.
     */
    static Set<String> graphSON1dDerivedTypeNames() {
        final Set<String> names = new HashSet<>();
        for (final Class c : registeredV2V3Types()) {
            // a java.lang.Class value resolves its own content as a class name, which GraphSON 1.0 does not need
            if (Class.class != c)
                names.add(c.getName());
        }
        return names;
    }

    private static boolean isAllowedTypeName(final String typeName, final Set<String> allowedNames) {
        // unwrap array descriptors: "[Ljava.lang.String;" -> "java.lang.String", "[[B" -> primitive element
        String name = typeName;
        while (name.startsWith("["))
            name = name.substring(1);
        if (name.length() <= 1)
            return true; // primitive array element (e.g. [B, [I) names no class
        if (name.startsWith("L") && name.endsWith(";"))
            name = name.substring(1, name.length() - 1);
        return allowedNames.contains(name);
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
        builder.pdtRegistry = mapper.pdtRegistry;
        builder.streamReadConstraintsBuilder = mapper.streamReadConstraints.rebuild();
        builder.allowedTypeIdNames.addAll(mapper.allowedTypeIdNames);

        return builder;
    }

    public TypeInfo getTypeInfo() {
        return this.typeInfo;
    }

    private void registerJavaBaseTypes(final GraphSONTypeIdResolver graphSONTypeIdResolver) {
        GRAPHSON_JAVA_BASE_TYPES.forEach(e -> graphSONTypeIdResolver.addCustomType(
                String.format("%s:%s", GraphSONTokens.GREMLIN_TYPE_NAMESPACE, e.getSimpleName()), e));
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
        private PDTRegistry pdtRegistry = null;
        private final List<String> allowedTypeIdNames = new ArrayList<>();

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

        /**
         * Set the {@link PDTRegistry} to enable automatic hydration of
         * {@link org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDT} values during deserialization.
         */
        public Builder pdtRegistry(final PDTRegistry pdtRegistry) {
            this.pdtRegistry = pdtRegistry;
            return this;
        }

        /**
         * Adds a fully qualified class name that GraphSON 1.0 embedded-type deserialization resolves in a
         * {@code @class} property, in addition to the default names. A name is matched by equality, so it covers the
         * class named and its arrays, but not its subclasses or its package. Adding the same name more than once has
         * no additional effect.
         * <p/>
         * Has no effect on GraphSON 2.0, 3.0 or 4.0, which resolve types through fixed registries rather than by name.
         */
        public Builder addAllowedTypeIdName(final String... names) {
            this.allowedTypeIdNames.addAll(Arrays.asList(names));
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
