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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded.ShadedSerializerAdapter;
import org.apache.tinkerpop.shaded.kryo.ClassResolver;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.util.DefaultStreamFactory;
import org.apache.tinkerpop.shaded.kryo.util.MapReferenceResolver;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link Mapper} implementation for Kryo. This implementation requires that all classes to be serialized by
 * Kryo are registered to it.
 * <p/>
 * {@link Graph} implementations providing an {@link IoRegistry} should register their custom classes and/or
 * serializers in one of three ways:
 * <p/>
 * <ol>
 * <li>Register just the custom class with a {@code null} {@link Serializer} implementation</li>
 * <li>Register the custom class with a {@link Serializer} implementation</li>
 * <li>
 * Register the custom class with a {@code Function<Kryo, Serializer>} for those cases where the
 * {@link Serializer} requires the {@link Kryo} instance to get constructed.
 * </li>
 * </ol>
 * <p/>
 * For example:
 * <pre>
 * {@code
 * public class MyGraphIoRegistry extends AbstractIoRegistry {
 *   public MyGraphIoRegistry() {
 *     register(GryoIo.class, MyGraphIdClass.class, new MyGraphIdSerializer());
 *   }
 * }
 * }
 * </pre>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GryoMapper implements Mapper<Kryo> {
    public static final byte[] GIO = "gio".getBytes();
    public static final byte[] HEADER = Arrays.copyOf(GIO, 16);
    private final List<TypeRegistration<?>> typeRegistrations;
    private final boolean registrationRequired;
    private final boolean referenceTracking;
    private final Supplier<ClassResolver> classResolver;
    private final GryoVersion version;

    private GryoMapper(final Builder builder) {
        this.typeRegistrations = builder.typeRegistrations;
        this.version = builder.version;
        validate();

        this.registrationRequired = builder.registrationRequired;
        this.referenceTracking = builder.referenceTracking;
        this.classResolver = null == builder.classResolver ? version.getClassResolverMaker() : builder.classResolver;
    }

    @Override
    public Kryo createMapper() {
        final Kryo kryo = new Kryo(classResolver.get(), new MapReferenceResolver(), new DefaultStreamFactory());
        kryo.addDefaultSerializer(Map.Entry.class, new UtilSerializers.EntrySerializer());
        kryo.setRegistrationRequired(registrationRequired);
        kryo.setReferences(referenceTracking);
        for (TypeRegistration tr : typeRegistrations)
            tr.registerWith(kryo);
        return kryo;
    }

    public GryoVersion getVersion() {
        return version;
    }

    public List<Class> getRegisteredClasses() {
        return this.typeRegistrations.stream().map(TypeRegistration::getTargetClass).collect(Collectors.toList());
    }

    public List<TypeRegistration<?>> getTypeRegistrations() {
        return typeRegistrations;
    }

    public static Builder build() {
        return new Builder();
    }

    private void validate() {
        final Set<Integer> duplicates = new HashSet<>();

        final Set<Integer> ids = new HashSet<>();
        typeRegistrations.forEach(t -> {
            if (!ids.contains(t.getId()))
                ids.add(t.getId());
            else
                duplicates.add(t.getId());
        });

        if (duplicates.size() > 0)
            throw new IllegalStateException("There are duplicate kryo identifiers in use: " + duplicates);
    }

    /**
     * A builder to construct a {@link GryoMapper} instance.
     */
    public static class Builder implements Mapper.Builder<Builder> {

        private GryoVersion version = GryoVersion.V3_0;

        /**
         * Note that the following are pre-registered boolean, Boolean, byte, Byte, char, Character, double, Double,
         * int, Integer, float, Float, long, Long, short, Short, String, void.
         */
        private List<TypeRegistration<?>> typeRegistrations = version.cloneRegistrations();

        private final List<IoRegistry> registries = new ArrayList<>();

        /**
         * Starts numbering classes for Gryo serialization at 65536 to leave room for future usage by TinkerPop.
         */
        private final AtomicInteger currentSerializationId = new AtomicInteger(65536);

        private boolean registrationRequired = true;
        private boolean referenceTracking = true;
        private Supplier<ClassResolver> classResolver;

        private Builder() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder addRegistry(final IoRegistry registry) {
            if (null == registry) throw new IllegalArgumentException("The registry cannot be null");
            this.registries.add(registry);
            return this;
        }

        /**
         * The version of Gryo to use in the mapper. Defaults to 1.0. Calls to this method will reset values specified
         * to {@link #addCustom(Class, Function)} and related overloads.
         */
        public Builder version(final GryoVersion version) {
            this.version = version;
            this.typeRegistrations = version.cloneRegistrations();
            return this;
        }

        /**
         * Provides a custom Kryo {@code ClassResolver} to be supplied to a {@code Kryo} instance.  If this value is
         * not supplied then it will default to the {@code ClassResolver} of the provided {@link GryoVersion}. To
         * ensure compatibility with Gryo it is highly recommended that objects passed to this method extend that class.
         * <p/>
         * If the {@code ClassResolver} implementation share state, then the {@link Supplier} should typically create
         * new instances when requested, as the {@link Supplier} will be called for each {@link Kryo} instance created.
         */
        public Builder classResolver(final Supplier<ClassResolver> classResolverSupplier) {
            if (null == classResolverSupplier)
                throw new IllegalArgumentException("The classResolverSupplier cannot be null");
            this.classResolver = classResolverSupplier;
            return this;
        }

        /**
         * Register custom classes to serializes with gryo using default serialization. Note that calling this method
         * for a class that is already registered will override that registration.
         */
        public Builder addCustom(final Class... custom) {
            if (custom != null && custom.length > 0) {
                for (Class c : custom) {
                    addOrOverrideRegistration(c, id -> GryoTypeReg.of(c, id));
                }
            }
            return this;
        }

        /**
         * Register custom class to serialize with a custom serialization class. Note that calling this method for
         * a class that is already registered will override that registration.
         */
        public Builder addCustom(final Class clazz, final Serializer serializer) {
            addOrOverrideRegistration(clazz, id -> GryoTypeReg.of(clazz, id, serializer));
            return this;
        }

        /**
         * Register custom class to serialize with a custom serialization shim.
         */
        public Builder addCustom(final Class clazz, final SerializerShim serializer) {
            addOrOverrideRegistration(clazz, id -> GryoTypeReg.of(clazz, id, serializer));
            return this;
        }

        /**
         * Register a custom class to serialize with a custom serializer as returned from a {@link Function}. Note
         * that calling this method for a class that is already registered will override that registration.
         */
        public Builder addCustom(final Class clazz, final Function<Kryo, Serializer> functionOfKryo) {
            addOrOverrideRegistration(clazz, id -> GryoTypeReg.of(clazz, id, functionOfKryo));
            return this;
        }

        /**
         * When set to {@code true}, all classes serialized by the {@code Kryo} instances created from this
         * {@link GryoMapper} must have their classes known up front and registered appropriately through this
         * builder.  By default this value is {@code true}.  This approach is more efficient than setting the
         * value to {@code false}.
         *
         * @param registrationRequired set to {@code true} if the classes should be registered up front or
         *                             {@code false} otherwise
         */
        public Builder registrationRequired(final boolean registrationRequired) {
            this.registrationRequired = registrationRequired;
            return this;
        }

        /**
         * By default, each appearance of an object in the graph after the first is stored as an integer ordinal.
         * This allows multiple references to the same object and cyclic graphs to be serialized. This has a small
         * amount of overhead and can be disabled to save space if it is not needed.
         *
         * @param referenceTracking set to {@code true} to enable and {@code false} otherwise
         */
        public Builder referenceTracking(final boolean referenceTracking) {
            this.referenceTracking = referenceTracking;
            return this;
        }

        /**
         * Creates a {@code GryoMapper}.
         */
        public GryoMapper create() {
            // consult the registry if provided and inject registry entries as custom classes.
            registries.forEach(registry -> {
                final List<Pair<Class, Object>> serializers = registry.find(GryoIo.class);
                serializers.forEach(p -> {
                    if (null == p.getValue1())
                        addCustom(p.getValue0());
                    else if (p.getValue1() instanceof SerializerShim)
                        addCustom(p.getValue0(), new ShadedSerializerAdapter((SerializerShim) p.getValue1()));
                    else if (p.getValue1() instanceof Serializer)
                        addCustom(p.getValue0(), (Serializer) p.getValue1());
                    else if (p.getValue1() instanceof Function)
                        addCustom(p.getValue0(), (Function<Kryo, Serializer>) p.getValue1());
                    else
                        throw new IllegalStateException(String.format(
                                "Unexpected value provided by %s for serializable class %s - expected a parameter in [null, %s implementation or Function<%s, %s>], but received %s",
                                registry.getClass().getSimpleName(), p.getValue0().getClass().getCanonicalName(),
                                Serializer.class.getName(), Kryo.class.getSimpleName(),
                                Serializer.class.getSimpleName(), p.getValue1()));
                });
            });

            return new GryoMapper(this);
        }

        private <T> void addOrOverrideRegistration(final Class<?> clazz,
                                                   final Function<Integer, TypeRegistration<T>> newRegistrationBuilder) {
            final Iterator<TypeRegistration<?>> iter = typeRegistrations.iterator();
            Integer registrationId = null;
            while (iter.hasNext()) {
                final TypeRegistration<?> existingRegistration = iter.next();
                if (existingRegistration.getTargetClass().equals(clazz)) {
                    // when overridding a registration, use the old id
                    registrationId = existingRegistration.getId();
                    // remove the old registration (we install its override below)
                    iter.remove();
                    break;
                }
            }
            if (null == registrationId) {
                // when not overridding a registration, get an id from the counter
                registrationId = currentSerializationId.getAndIncrement();
            }
            typeRegistrations.add(newRegistrationBuilder.apply(registrationId));
        }
    }

    private static class GryoTypeReg<T> implements TypeRegistration<T> {

        private final Class<T> clazz;
        private final Serializer<T> shadedSerializer;
        private final SerializerShim<T> serializerShim;
        private final Function<Kryo, Serializer> functionOfShadedKryo;
        private final int id;

        private GryoTypeReg(final Class<T> clazz,
                            final Serializer<T> shadedSerializer,
                            final SerializerShim<T> serializerShim,
                            final Function<Kryo, Serializer> functionOfShadedKryo,
                            final int id) {
            this.clazz = clazz;
            this.shadedSerializer = shadedSerializer;
            this.serializerShim = serializerShim;
            this.functionOfShadedKryo = functionOfShadedKryo;
            this.id = id;

            int serializerCount = 0;
            if (null != this.shadedSerializer)
                serializerCount++;
            if (null != this.serializerShim)
                serializerCount++;
            if (null != this.functionOfShadedKryo)
                serializerCount++;

            if (1 < serializerCount) {
                final String msg = String.format(
                        "GryoTypeReg accepts at most one kind of serializer, but multiple " +
                                "serializers were supplied for class %s (id %s).  " +
                                "Shaded serializer: %s.  Shim serializer: %s.  Shaded serializer function: %s.",
                        this.clazz.getCanonicalName(), id,
                        this.shadedSerializer, this.serializerShim, this.functionOfShadedKryo);
                throw new IllegalArgumentException(msg);
            }
        }

        private static <T> GryoTypeReg<T> of(final Class<T> clazz, final int id) {
            return new GryoTypeReg<>(clazz, null, null, null, id);
        }

        private static <T> GryoTypeReg<T> of(final Class<T> clazz, final int id, final Serializer<T> shadedSerializer) {
            return new GryoTypeReg<>(clazz, shadedSerializer, null, null, id);
        }

        private static <T> GryoTypeReg<T> of(final Class<T> clazz, final int id, final SerializerShim<T> serializerShim) {
            return new GryoTypeReg<>(clazz, null, serializerShim, null, id);
        }

        private static <T> GryoTypeReg<T> of(final Class clazz, final int id, final Function<Kryo, Serializer> fct) {
            return new GryoTypeReg<>(clazz, null, null, fct, id);
        }

        @Override
        public Serializer<T> getShadedSerializer() {
            return shadedSerializer;
        }

        @Override
        public SerializerShim<T> getSerializerShim() {
            return serializerShim;
        }

        @Override
        public Function<Kryo, Serializer> getFunctionOfShadedKryo() {
            return functionOfShadedKryo;
        }

        @Override
        public Class<T> getTargetClass() {
            return clazz;
        }

        @Override
        public int getId() {
            return id;
        }

        @Override
        public Kryo registerWith(final Kryo kryo) {
            if (null != functionOfShadedKryo)
                kryo.register(clazz, functionOfShadedKryo.apply(kryo), id);
            else if (null != shadedSerializer)
                kryo.register(clazz, shadedSerializer, id);
            else if (null != serializerShim)
                kryo.register(clazz, new ShadedSerializerAdapter<>(serializerShim), id);
            else {
                kryo.register(clazz, kryo.getDefaultSerializer(clazz), id);
                // Suprisingly, the preceding call is not equivalent to
                //   kryo.register(clazz, id);
            }

            return kryo;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("targetClass", clazz)
                    .append("id", id)
                    .append("shadedSerializer", shadedSerializer)
                    .append("serializerShim", serializerShim)
                    .append("functionOfShadedKryo", functionOfShadedKryo)
                    .toString();
        }
    }
}
