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
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStepV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.ImmutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded.ShadedSerializerAdapter;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphSerializer;
import org.apache.tinkerpop.gremlin.util.function.HashSetSupplier;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.shaded.kryo.ClassResolver;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.KryoSerializable;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.serializers.JavaSerializer;
import org.apache.tinkerpop.shaded.kryo.util.DefaultStreamFactory;
import org.apache.tinkerpop.shaded.kryo.util.MapReferenceResolver;
import org.javatuples.Pair;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

    private GryoMapper(final Builder builder) {
        this.typeRegistrations = builder.typeRegistrations;
        validate();

        this.registrationRequired = builder.registrationRequired;
        this.referenceTracking = builder.referenceTracking;
        this.classResolver = builder.classResolver;
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
    public final static class Builder implements Mapper.Builder<Builder> {

        /**
         * Map with one entry that is used so that it is possible to get the class of LinkedHashMap.Entry.
         */
        private static final LinkedHashMap m = new LinkedHashMap() {{
            put("junk", "dummy");
        }};

        private static final Class ARRAYS_AS_LIST = Arrays.asList("dummy").getClass();

        private static final Class LINKED_HASH_MAP_ENTRY_CLASS = m.entrySet().iterator().next().getClass();

        /**
         * The {@code HashMap$Node} class comes into serialization play when a {@code Map.entrySet()} is
         * serialized.
         */
        private static final Class HASH_MAP_NODE;

        static {
            // have to instantiate this via reflection because it is a private inner class of HashMap
            final String className = HashMap.class.getName() + "$Node";
            try {
                HASH_MAP_NODE = Class.forName(className);
            } catch (Exception ex) {
                throw new RuntimeException("Could not access " + className, ex);
            }
        }

        /**
         * Note that the following are pre-registered boolean, Boolean, byte, Byte, char, Character, double, Double,
         * int, Integer, float, Float, long, Long, short, Short, String, void.
         */
        private final List<TypeRegistration<?>> typeRegistrations = new ArrayList<TypeRegistration<?>>() {{
            add(GryoTypeReg.of(byte[].class, 25));
            add(GryoTypeReg.of(char[].class, 26));
            add(GryoTypeReg.of(short[].class, 27));
            add(GryoTypeReg.of(int[].class, 28));
            add(GryoTypeReg.of(long[].class, 29));
            add(GryoTypeReg.of(float[].class, 30));
            add(GryoTypeReg.of(double[].class, 31));
            add(GryoTypeReg.of(String[].class, 32));
            add(GryoTypeReg.of(Object[].class, 33));
            add(GryoTypeReg.of(ArrayList.class, 10));
            add(GryoTypeReg.of(ARRAYS_AS_LIST, 134, new UtilSerializers.ArraysAsListSerializer()));
            add(GryoTypeReg.of(BigInteger.class, 34));
            add(GryoTypeReg.of(BigDecimal.class, 35));
            add(GryoTypeReg.of(Calendar.class, 39));
            add(GryoTypeReg.of(Class.class, 41));
            add(GryoTypeReg.of(Collection.class, 37));
            add(GryoTypeReg.of(Collections.EMPTY_LIST.getClass(), 51));
            add(GryoTypeReg.of(Collections.EMPTY_MAP.getClass(), 52));
            add(GryoTypeReg.of(Collections.EMPTY_SET.getClass(), 53));
            add(GryoTypeReg.of(Collections.singleton(null).getClass(), 54));
            add(GryoTypeReg.of(Collections.singletonList(null).getClass(), 24));
            add(GryoTypeReg.of(Collections.singletonMap(null, null).getClass(), 23));
            add(GryoTypeReg.of(Contains.class, 49));
            add(GryoTypeReg.of(Currency.class, 40));
            add(GryoTypeReg.of(Date.class, 38));
            add(GryoTypeReg.of(Direction.class, 12));
            add(GryoTypeReg.of(DetachedEdge.class, 21));
            add(GryoTypeReg.of(DetachedVertexProperty.class, 20));
            add(GryoTypeReg.of(DetachedProperty.class, 18));
            add(GryoTypeReg.of(DetachedVertex.class, 19));
            add(GryoTypeReg.of(DetachedPath.class, 60));
            // skip 14
            add(GryoTypeReg.of(EnumSet.class, 46));
            add(GryoTypeReg.of(HashMap.class, 11));
            add(GryoTypeReg.of(HashMap.Entry.class, 16));
            add(GryoTypeReg.of(HASH_MAP_NODE, 92));
            add(GryoTypeReg.of(KryoSerializable.class, 36));
            add(GryoTypeReg.of(LinkedHashMap.class, 47));
            add(GryoTypeReg.of(LinkedHashSet.class, 71));
            add(GryoTypeReg.of(LinkedList.class, 116));
            add(GryoTypeReg.of(LINKED_HASH_MAP_ENTRY_CLASS, 15));
            add(GryoTypeReg.of(Locale.class, 22));
            add(GryoTypeReg.of(StringBuffer.class, 43));
            add(GryoTypeReg.of(StringBuilder.class, 44));
            add(GryoTypeReg.of(T.class, 48));
            add(GryoTypeReg.of(TimeZone.class, 42));
            add(GryoTypeReg.of(TreeMap.class, 45));
            add(GryoTypeReg.of(TreeSet.class, 50));
            add(GryoTypeReg.of(UUID.class, 17, new UtilSerializers.UUIDSerializer()));
            add(GryoTypeReg.of(URI.class, 72, new UtilSerializers.URISerializer()));
            add(GryoTypeReg.of(VertexTerminator.class, 13));
            add(GryoTypeReg.of(AbstractMap.SimpleEntry.class, 120));
            add(GryoTypeReg.of(AbstractMap.SimpleImmutableEntry.class, 121));

            add(GryoTypeReg.of(ReferenceEdge.class, 81));
            add(GryoTypeReg.of(ReferenceVertexProperty.class, 82));
            add(GryoTypeReg.of(ReferenceProperty.class, 83));
            add(GryoTypeReg.of(ReferenceVertex.class, 84));
            add(GryoTypeReg.of(ReferencePath.class, 85));

            add(GryoTypeReg.of(StarGraph.class, 86, new StarGraphSerializer(Direction.BOTH, new GraphFilter())));

            add(GryoTypeReg.of(Edge.class, 65, new GryoSerializers.EdgeSerializer()));
            add(GryoTypeReg.of(Vertex.class, 66, new GryoSerializers.VertexSerializer()));
            add(GryoTypeReg.of(Property.class, 67, new GryoSerializers.PropertySerializer()));
            add(GryoTypeReg.of(VertexProperty.class, 68, new GryoSerializers.VertexPropertySerializer()));
            add(GryoTypeReg.of(Path.class, 59, new GryoSerializers.PathSerializer()));
            // skip 55
            add(GryoTypeReg.of(B_O_Traverser.class, 75));
            add(GryoTypeReg.of(O_Traverser.class, 76));
            add(GryoTypeReg.of(B_LP_O_P_S_SE_SL_Traverser.class, 77));
            add(GryoTypeReg.of(B_O_S_SE_SL_Traverser.class, 78));
            add(GryoTypeReg.of(B_LP_O_S_SE_SL_Traverser.class, 87));
            add(GryoTypeReg.of(O_OB_S_SE_SL_Traverser.class, 89));
            add(GryoTypeReg.of(LP_O_OB_S_SE_SL_Traverser.class, 90));
            add(GryoTypeReg.of(LP_O_OB_P_S_SE_SL_Traverser.class, 91));
            add(GryoTypeReg.of(DefaultRemoteTraverser.class, 123, new GryoSerializers.DefaultRemoteTraverserSerializer()));

            add(GryoTypeReg.of(Bytecode.class, 122, new GryoSerializers.BytecodeSerializer()));
            add(GryoTypeReg.of(P.class, 124, new GryoSerializers.PSerializer()));
            add(GryoTypeReg.of(Lambda.class, 125, new GryoSerializers.LambdaSerializer()));
            add(GryoTypeReg.of(Bytecode.Binding.class, 126, new GryoSerializers.BindingSerializer()));
            add(GryoTypeReg.of(Order.class, 127));
            add(GryoTypeReg.of(Scope.class, 128));
            add(GryoTypeReg.of(AndP.class, 129, new GryoSerializers.AndPSerializer()));
            add(GryoTypeReg.of(OrP.class, 130, new GryoSerializers.OrPSerializer()));
            add(GryoTypeReg.of(VertexProperty.Cardinality.class, 131));
            add(GryoTypeReg.of(Column.class, 132));
            add(GryoTypeReg.of(Pop.class, 133));
            add(GryoTypeReg.of(SackFunctions.Barrier.class, 135));
            add(GryoTypeReg.of(TraversalOptionParent.Pick.class, 137)); // ***LAST ID***
            add(GryoTypeReg.of(HashSetSupplier.class, 136, new UtilSerializers.HashSetSupplierSerializer()));

            add(GryoTypeReg.of(TraverserSet.class, 58));
            add(GryoTypeReg.of(Tree.class, 61));
            add(GryoTypeReg.of(HashSet.class, 62));
            add(GryoTypeReg.of(BulkSet.class, 64));
            add(GryoTypeReg.of(MutableMetrics.class, 69));
            add(GryoTypeReg.of(ImmutableMetrics.class, 115));
            add(GryoTypeReg.of(DefaultTraversalMetrics.class, 70));
            add(GryoTypeReg.of(MapMemory.class, 73));
            add(GryoTypeReg.of(MapReduce.NullObject.class, 74));
            add(GryoTypeReg.of(AtomicLong.class, 79));
            add(GryoTypeReg.of(Pair.class, 88, new UtilSerializers.PairSerializer()));
            add(GryoTypeReg.of(TraversalExplanation.class, 106, new JavaSerializer()));

            add(GryoTypeReg.of(Duration.class, 93, new JavaTimeSerializers.DurationSerializer()));
            add(GryoTypeReg.of(Instant.class, 94, new JavaTimeSerializers.InstantSerializer()));
            add(GryoTypeReg.of(LocalDate.class, 95, new JavaTimeSerializers.LocalDateSerializer()));
            add(GryoTypeReg.of(LocalDateTime.class, 96, new JavaTimeSerializers.LocalDateTimeSerializer()));
            add(GryoTypeReg.of(LocalTime.class, 97, new JavaTimeSerializers.LocalTimeSerializer()));
            add(GryoTypeReg.of(MonthDay.class, 98, new JavaTimeSerializers.MonthDaySerializer()));
            add(GryoTypeReg.of(OffsetDateTime.class, 99, new JavaTimeSerializers.OffsetDateTimeSerializer()));
            add(GryoTypeReg.of(OffsetTime.class, 100, new JavaTimeSerializers.OffsetTimeSerializer()));
            add(GryoTypeReg.of(Period.class, 101, new JavaTimeSerializers.PeriodSerializer()));
            add(GryoTypeReg.of(Year.class, 102, new JavaTimeSerializers.YearSerializer()));
            add(GryoTypeReg.of(YearMonth.class, 103, new JavaTimeSerializers.YearMonthSerializer()));
            add(GryoTypeReg.of(ZonedDateTime.class, 104, new JavaTimeSerializers.ZonedDateTimeSerializer()));
            add(GryoTypeReg.of(ZoneOffset.class, 105, new JavaTimeSerializers.ZoneOffsetSerializer()));

            add(GryoTypeReg.of(Operator.class, 107));
            add(GryoTypeReg.of(FoldStep.FoldBiOperator.class, 108));
            add(GryoTypeReg.of(GroupCountStep.GroupCountBiOperator.class, 109));
            add(GryoTypeReg.of(GroupStep.GroupBiOperator.class, 117, new JavaSerializer())); // because they contain traversals
            add(GryoTypeReg.of(MeanGlobalStep.MeanGlobalBiOperator.class, 110));
            add(GryoTypeReg.of(MeanGlobalStep.MeanNumber.class, 111));
            add(GryoTypeReg.of(TreeStep.TreeBiOperator.class, 112));
            add(GryoTypeReg.of(GroupStepV3d0.GroupBiOperatorV3d0.class, 113));
            add(GryoTypeReg.of(RangeGlobalStep.RangeBiOperator.class, 114));
            add(GryoTypeReg.of(OrderGlobalStep.OrderBiOperator.class, 118, new JavaSerializer())); // because they contain traversals
            add(GryoTypeReg.of(ProfileStep.ProfileBiOperator.class, 119));
        }};

        /**
         * Starts numbering classes for Gryo serialization at 65536 to leave room for future usage by TinkerPop.
         */
        private final AtomicInteger currentSerializationId = new AtomicInteger(65536);

        private boolean registrationRequired = true;
        private boolean referenceTracking = true;
        private Supplier<ClassResolver> classResolver = GryoClassResolver::new;

        private Builder() {
            // Validate the default registrations
            // For justification of these default registration rules, see TinkerPopKryoRegistrator
            for (TypeRegistration<?> tr : typeRegistrations) {
                if (tr.hasSerializer() /* no serializer is acceptable */ &&
                        null == tr.getSerializerShim() /* a shim serializer is acceptable */ &&
                        !(tr.getShadedSerializer() instanceof JavaSerializer) /* shaded JavaSerializer is acceptable */) {
                    // everything else is invalid
                    final String msg = String.format("The default GryoMapper type registration %s is invalid.  " +
                                    "It must supply either an implementation of %s or %s, but supplies neither.  " +
                                    "This is probably a bug in GryoMapper's default serialization class registrations.", tr,
                            SerializerShim.class.getCanonicalName(), JavaSerializer.class.getCanonicalName());
                    throw new IllegalStateException(msg);
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder addRegistry(final IoRegistry registry) {
            if (null == registry) throw new IllegalArgumentException("The registry cannot be null");
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
                            "Unexpected value provided by %s for serializable class %s - expected a parameter in [null, %s (or shim) implementation or Function<%s, %s>], but received %s",
                            registry.getClass().getSimpleName(), p.getValue0().getClass().getCanonicalName(),
                            Serializer.class.getName(), Kryo.class.getSimpleName(),
                            Serializer.class.getSimpleName(), p.getValue1()));
            });
            return this;
        }

        /**
         * Provides a custom Kryo {@code ClassResolver} to be supplied to a {@code Kryo} instance.  If this value is
         * not supplied then it will default to the {@link GryoClassResolver}. To ensure compatibility with Gryo it
         * is highly recommended that objects passed to this method extend that class.
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
