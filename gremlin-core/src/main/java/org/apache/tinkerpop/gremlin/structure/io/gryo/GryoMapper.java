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

import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.DependantMutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
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
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGryoSerializer;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.KryoSerializable;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.util.DefaultStreamFactory;
import org.apache.tinkerpop.shaded.kryo.util.MapReferenceResolver;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
    private final List<Triplet<Class, Function<Kryo, Serializer>, Integer>> serializationList;
    private boolean registrationRequired;
    private boolean referenceTracking;

    private GryoMapper(final Builder builder) {
        this.serializationList = builder.serializationList;
        this.registrationRequired = builder.registrationRequired;
        this.referenceTracking = builder.referenceTracking;
    }

    @Override
    public Kryo createMapper() {
        final Kryo kryo = new Kryo(new GryoClassResolver(), new MapReferenceResolver(), new DefaultStreamFactory());
        kryo.addDefaultSerializer(Map.Entry.class, new EntrySerializer());
        kryo.setRegistrationRequired(registrationRequired);
        kryo.setReferences(referenceTracking);

        serializationList.forEach(p -> {
            final Function<Kryo, Serializer> serializer = p.getValue1();
            if (null == serializer)
                kryo.register(p.getValue0(), kryo.getDefaultSerializer(p.getValue0()), p.getValue2());
            else
                kryo.register(p.getValue0(), serializer.apply(kryo), p.getValue2());
        });
        return kryo;
    }

    public List<Class> getRegisteredClasses() {
        return this.serializationList.stream().map(Triplet::getValue0).collect(Collectors.toList());
    }

    public static Builder build() {
        return new Builder();
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
        private final List<Triplet<Class, Function<Kryo, Serializer>, Integer>> serializationList = new ArrayList<Triplet<Class, Function<Kryo, Serializer>, Integer>>() {{
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(byte[].class, null, 25));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(char[].class, null, 26));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(short[].class, null, 27));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(int[].class, null, 28));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(long[].class, null, 29));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(float[].class, null, 30));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(double[].class, null, 31));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(String[].class, null, 32));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Object[].class, null, 33));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ArrayList.class, null, 10));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(BigInteger.class, null, 34));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(BigDecimal.class, null, 35));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Calendar.class, null, 39));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Class.class, null, 41));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collection.class, null, 37));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.EMPTY_LIST.getClass(), null, 51));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.EMPTY_MAP.getClass(), null, 52));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.EMPTY_SET.getClass(), null, 53));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.singleton(null).getClass(), null, 54));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.singletonList(null).getClass(), null, 24));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Collections.singletonMap(null, null).getClass(), null, 23));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Contains.class, null, 49));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Currency.class, null, 40));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Date.class, null, 38));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Direction.class, null, 12));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DetachedEdge.class, null, 21));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DetachedVertexProperty.class, null, 20));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DetachedProperty.class, null, 18));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DetachedVertex.class, null, 19));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DetachedPath.class, null, 60));
            // skip 14
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(EnumSet.class, null, 46));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(HashMap.class, null, 11));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(HashMap.Entry.class, null, 16));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(HASH_MAP_NODE, null, 92));   // ***LAST ID**
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(KryoSerializable.class, null, 36));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(LinkedHashMap.class, null, 47));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(LinkedHashSet.class, null, 71));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(LINKED_HASH_MAP_ENTRY_CLASS, null, 15));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Locale.class, null, 22));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(StringBuffer.class, null, 43));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(StringBuilder.class, null, 44));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(T.class, null, 48));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(TimeZone.class, null, 42));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(TreeMap.class, null, 45));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(TreeSet.class, null, 50));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(UUID.class, kryo -> new UUIDSerializer(), 17));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(URI.class, kryo -> new URISerializer(), 72));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(VertexTerminator.class, null, 13));

            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ReferenceEdge.class, null, 81));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ReferenceVertexProperty.class, null, 82));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ReferenceProperty.class, null, 83));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ReferenceVertex.class, null, 84));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(ReferencePath.class, null, 85));

            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(StarGraph.class, kryo -> StarGraphGryoSerializer.with(Direction.BOTH), 86));

            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Edge.class, kryo -> new GryoSerializers.EdgeSerializer(), 65));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Vertex.class, kryo -> new GryoSerializers.VertexSerializer(), 66));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Property.class, kryo -> new GryoSerializers.PropertySerializer(), 67));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(VertexProperty.class, kryo -> new GryoSerializers.VertexPropertySerializer(), 68));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Path.class, kryo -> new GryoSerializers.PathSerializer(), 59));
            // skip 55
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(B_O_Traverser.class, null, 75));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(O_Traverser.class, null, 76));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(B_LP_O_P_S_SE_SL_Traverser.class, null, 77));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(B_O_S_SE_SL_Traverser.class, null, 78));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(B_LP_O_S_SE_SL_Traverser.class, null, 87));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(O_OB_S_SE_SL_Traverser.class, null, 89));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(LP_O_OB_S_SE_SL_Traverser.class, null, 90));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(LP_O_OB_P_S_SE_SL_TraverserGenerator.class, null, 91));

            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(TraverserSet.class, null, 58));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Tree.class, null, 61));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(HashSet.class, null, 62));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(BulkSet.class, null, 64));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(MutableMetrics.class, null, 69));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(StandardTraversalMetrics.class, null, 70));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(MapMemory.class, null, 73));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(MapReduce.NullObject.class, null, 74));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(AtomicLong.class, null, 79));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(DependantMutableMetrics.class, null, 80));
            add(Triplet.<Class, Function<Kryo, Serializer>, Integer>with(Pair.class, kryo -> new PairSerializer(), 88));
        }};

        private final List<IoRegistry> registries = new ArrayList<>();

        /**
         * Starts numbering classes for Gryo serialization at 65536 to leave room for future usage by TinkerPop.
         */
        private final AtomicInteger currentSerializationId = new AtomicInteger(65536);

        private boolean registrationRequired = true;
        private boolean referenceTracking = true;

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
         * Register custom classes to serializes with gryo using default serialization.
         */
        public Builder addCustom(final Class... custom) {
            if (custom != null && custom.length > 0)
                serializationList.addAll(Arrays.asList(custom).stream()
                        .map(c -> Triplet.<Class, Function<Kryo, Serializer>, Integer>with(c, null, currentSerializationId.getAndIncrement()))
                        .collect(Collectors.<Triplet<Class, Function<Kryo, Serializer>, Integer>>toList()));
            return this;
        }

        /**
         * Register custom class to serialize with a custom serialization class.
         */
        public Builder addCustom(final Class clazz, final Serializer serializer) {
            serializationList.add(Triplet.with(clazz, kryo -> serializer, currentSerializationId.getAndIncrement()));
            return this;
        }

        /**
         * Register a custom class to serialize with a custom serializer as returned from a {@link Function}.
         */
        public Builder addCustom(final Class clazz, final Function<Kryo, Serializer> serializer) {
            serializationList.add(Triplet.with(clazz, serializer, currentSerializationId.getAndIncrement()));
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
                    else if (p.getValue1() instanceof Serializer)
                        addCustom(p.getValue0(), (Serializer) p.getValue1());
                    else if (p.getValue1() instanceof Function)
                        addCustom(p.getValue0(), (Function<Kryo, Serializer>) p.getValue1());
                    else
                        throw new IllegalStateException(String.format(
                                "Unexpected value provided by the %s for %s - expects [null, %s implementation or Function<%s, %s>]",
                                IoRegistry.class.getSimpleName(), p.getValue0().getClass().getSimpleName(),
                                Serializer.class.getName(), Kryo.class.getSimpleName(),
                                Serializer.class.getSimpleName()));
                });
            });

            return new GryoMapper(this);
        }
    }
}
