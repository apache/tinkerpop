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

import org.apache.commons.collections4.map.ReferenceMap;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.step.GType;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ByModulatorOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.OrderLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathProcessorStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_NL_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.NL_O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.IndexedTraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.LabelledCounter;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.ImmutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
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
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.tinkerpop.gremlin.util.function.HashSetSupplier;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.function.MultiComparator;
import org.apache.tinkerpop.shaded.kryo.ClassResolver;
import org.apache.tinkerpop.shaded.kryo.KryoSerializable;
import org.apache.tinkerpop.shaded.kryo.serializers.JavaSerializer;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Stack;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum GryoVersion {
    V1_0("1.0", initV1Registrations(), GryoClassResolverV1::new),
    V3_0("3.0", initV3Registrations(), GryoClassResolverV3::new);

    private final String versionNumber;
    private final List<TypeRegistration<?>> registrations;
    private final Supplier<ClassResolver> classResolverMaker;

    /**
     * Creates a new {@link GryoVersion}.
     *
     * @param versionNumber the user facing string representation of the version which should follow an {@code x.y}
     *                      pattern
     * @param registrations the list of registrations for this version
     * @param classResolverMaker providers the default {@code ClassResolver} for a particular version of Gryo
     */
    GryoVersion(final String versionNumber, final List<TypeRegistration<?>> registrations, final Supplier<ClassResolver> classResolverMaker) {
        // Validate the default registrations
        // For justification of these default registration rules, see TinkerPopKryoRegistrator
        for (TypeRegistration<?> tr : registrations) {
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

        this.versionNumber = versionNumber;
        this.registrations = registrations;
        this.classResolverMaker = classResolverMaker;
    }

    public List<TypeRegistration<?>> cloneRegistrations() {
        return new ArrayList<>(registrations);
    }

    public List<TypeRegistration<?>> getRegistrations() {
        return Collections.unmodifiableList(registrations);
    }

    public Supplier<ClassResolver> getClassResolverMaker() {
        return classResolverMaker;
    }

    public String getVersion() {
        return versionNumber;
    }

    public static List<TypeRegistration<?>> initV3Registrations() {
        return new ArrayList<TypeRegistration<?>>() {{
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
            add(GryoTypeReg.of(Types.ARRAYS_AS_LIST, 134, new UtilSerializers.ArraysAsListSerializer()));
            add(GryoTypeReg.of(BigInteger.class, 34));
            add(GryoTypeReg.of(BigDecimal.class, 35));
            add(GryoTypeReg.of(Calendar.class, 39));
            add(GryoTypeReg.of(Class.class, 41, new UtilSerializers.ClassSerializer()));
            add(GryoTypeReg.of(Class[].class, 166, new UtilSerializers.ClassArraySerializer()));
            add(GryoTypeReg.of(Collection.class, 37));
            add(GryoTypeReg.of(Collections.EMPTY_LIST.getClass(), 51));
            add(GryoTypeReg.of(Collections.EMPTY_MAP.getClass(), 52));
            add(GryoTypeReg.of(Collections.EMPTY_SET.getClass(), 53));
            add(GryoTypeReg.of(Collections.singleton(null).getClass(), 54));
            add(GryoTypeReg.of(Collections.singletonList(null).getClass(), 24));
            add(GryoTypeReg.of(Collections.singletonMap(null, null).getClass(), 23));
            add(GryoTypeReg.of(Types.COLLECTIONS_SYNCHRONIZED_MAP, 185, new UtilSerializers.SynchronizedMapSerializer()));
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
            add(GryoTypeReg.of(Types.HASH_MAP_NODE, 92));
            add(GryoTypeReg.of(Types.HASH_MAP_TREE_NODE, 172));
            add(GryoTypeReg.of(KryoSerializable.class, 36));
            add(GryoTypeReg.of(LinkedHashMap.class, 47));
            add(GryoTypeReg.of(LinkedHashSet.class, 71));
            add(GryoTypeReg.of(ConcurrentHashMap.class, 170));
            add(GryoTypeReg.of(ConcurrentHashMap.Entry.class, 171));
            add(GryoTypeReg.of(LinkedList.class, 116));
            add(GryoTypeReg.of(Types.LINKED_HASH_MAP_ENTRY_CLASS, 15));
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
            add(GryoTypeReg.of(java.sql.Timestamp.class, 161));
            add(GryoTypeReg.of(InetAddress.class, 162, new UtilSerializers.InetAddressSerializer()));
            add(GryoTypeReg.of(ByteBuffer.class, 163, new UtilSerializers.ByteBufferSerializer()));

            add(GryoTypeReg.of(ReferenceEdge.class, 81));
            add(GryoTypeReg.of(ReferenceVertexProperty.class, 82));
            add(GryoTypeReg.of(ReferenceProperty.class, 83));
            add(GryoTypeReg.of(ReferenceVertex.class, 84));
            add(GryoTypeReg.of(ReferencePath.class, 85));

            add(GryoTypeReg.of(StarGraph.class, 86, new StarGraphSerializer(Direction.BOTH, new GraphFilter())));

            add(GryoTypeReg.of(Edge.class, 65, new GryoSerializersV3.EdgeSerializer()));
            add(GryoTypeReg.of(Vertex.class, 66, new GryoSerializersV3.VertexSerializer()));
            add(GryoTypeReg.of(Property.class, 67, new GryoSerializersV3.PropertySerializer()));
            add(GryoTypeReg.of(VertexProperty.class, 68, new GryoSerializersV3.VertexPropertySerializer()));
            add(GryoTypeReg.of(Path.class, 59, new GryoSerializersV3.PathSerializer()));
            // skip 55
            add(GryoTypeReg.of(B_O_Traverser.class, 75));
            add(GryoTypeReg.of(O_Traverser.class, 76));
            add(GryoTypeReg.of(B_LP_O_P_S_SE_SL_Traverser.class, 77));
            add(GryoTypeReg.of(B_O_S_SE_SL_Traverser.class, 78));
            add(GryoTypeReg.of(B_LP_O_S_SE_SL_Traverser.class, 87));
            add(GryoTypeReg.of(O_OB_S_SE_SL_Traverser.class, 89));
            add(GryoTypeReg.of(LP_O_OB_S_SE_SL_Traverser.class, 90));
            add(GryoTypeReg.of(LP_O_OB_P_S_SE_SL_Traverser.class, 91));

            add(GryoTypeReg.of(ProjectedTraverser.class, 168));
            add(GryoTypeReg.of(DefaultRemoteTraverser.class, 123, new GryoSerializersV3.DefaultRemoteTraverserSerializer()));

            // 122 was Bytecode until it was removed for 4.x
            // 126 was Bytecode.Binding until it was removed 4.x

            add(GryoTypeReg.of(P.class, 124, new GryoSerializersV3.PSerializer()));
            add(GryoTypeReg.of(TextP.class, 186, new GryoSerializersV3.TextPSerializer()));
            add(GryoTypeReg.of(Text.RegexPredicate.class, 197));
            add(GryoTypeReg.of(Lambda.class, 125, new GryoSerializersV3.LambdaSerializer()));
            add(GryoTypeReg.of(Order.class, 127));
            add(GryoTypeReg.of(Scope.class, 128));
            add(GryoTypeReg.of(VertexProperty.Cardinality.class, 131));
            add(GryoTypeReg.of(Column.class, 132));
            add(GryoTypeReg.of(Pop.class, 133));
            add(GryoTypeReg.of(SackFunctions.Barrier.class, 135));
            add(GryoTypeReg.of(Pick.class, 137));
            add(GryoTypeReg.of(DT.class, 198));
            add(GryoTypeReg.of(Merge.class, 196));
            add(GryoTypeReg.of(HashSetSupplier.class, 136, new UtilSerializers.HashSetSupplierSerializer()));
            add(GryoTypeReg.of(MultiComparator.class, 165));

            add(GryoTypeReg.of(ConnectiveStrategy.class, 138));
            add(GryoTypeReg.of(HaltedTraverserStrategy.class, 139));
            add(GryoTypeReg.of(PartitionStrategy.class, 140, new JavaSerializer()));
            add(GryoTypeReg.of(SubgraphStrategy.class, 141, new JavaSerializer()));
            add(GryoTypeReg.of(SeedStrategy.class, 192, new JavaSerializer()));
            add(GryoTypeReg.of(VertexProgramStrategy.class, 142, new JavaSerializer()));
            add(GryoTypeReg.of(MatchAlgorithmStrategy.class, 143));
            add(GryoTypeReg.of(MatchStep.GreedyMatchAlgorithm.class, 144));
            add(GryoTypeReg.of(AdjacentToIncidentStrategy.class, 145));
            add(GryoTypeReg.of(ByModulatorOptimizationStrategy.class, 191));
            add(GryoTypeReg.of(ProductiveByStrategy.class, 195, new JavaSerializer()));
            add(GryoTypeReg.of(CountStrategy.class, 155));
            add(GryoTypeReg.of(FilterRankingStrategy.class, 146));
            add(GryoTypeReg.of(IdentityRemovalStrategy.class, 147));
            add(GryoTypeReg.of(IncidentToAdjacentStrategy.class, 148));
            add(GryoTypeReg.of(InlineFilterStrategy.class, 149));
            add(GryoTypeReg.of(LazyBarrierStrategy.class, 150));
            add(GryoTypeReg.of(MatchPredicateStrategy.class, 151));
            add(GryoTypeReg.of(OrderLimitStrategy.class, 152));
            add(GryoTypeReg.of(OptionsStrategy.class, 187, new JavaSerializer()));
            add(GryoTypeReg.of(PathProcessorStrategy.class, 153));
            add(GryoTypeReg.of(PathRetractionStrategy.class, 154));
            add(GryoTypeReg.of(RepeatUnrollStrategy.class, 156));
            add(GryoTypeReg.of(GraphFilterStrategy.class, 157));
            add(GryoTypeReg.of(LambdaRestrictionStrategy.class, 158));
            add(GryoTypeReg.of(ReadOnlyStrategy.class, 159));
            add(GryoTypeReg.of(EarlyLimitStrategy.class, 188));
            add(GryoTypeReg.of(MatchStep.CountMatchAlgorithm.class, 160));
            add(GryoTypeReg.of(MatchStep.GreedyMatchAlgorithm.class, 164));
            add(GryoTypeReg.of(EdgeLabelVerificationStrategy.class, 189));
            add(GryoTypeReg.of(ReservedKeysVerificationStrategy.class, 190));

            add(GryoTypeReg.of(TraverserSet.class, 58));
            add(GryoTypeReg.of(Tree.class, 61));
            add(GryoTypeReg.of(HashSet.class, 62));
            add(GryoTypeReg.of(BulkSet.class, 64));
            add(GryoTypeReg.of(Metrics.class, 69, new GryoSerializersV3.MetricsSerializer()));
            add(GryoTypeReg.of(TraversalMetrics.class, 70, new GryoSerializersV3.TraversalMetricsSerializer()));
            add(GryoTypeReg.of(MapMemory.class, 73));
            add(GryoTypeReg.of(MapReduce.NullObject.class, 74));
            add(GryoTypeReg.of(AtomicLong.class, 79));
            add(GryoTypeReg.of(Pair.class, 88, new UtilSerializers.PairSerializer()));
            add(GryoTypeReg.of(Triplet.class, 183, new UtilSerializers.TripletSerializer()));
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
            add(GryoTypeReg.of(GroupStep.GroupBiOperator.class, 117));
            add(GryoTypeReg.of(MeanGlobalStep.MeanGlobalBiOperator.class, 110));
            add(GryoTypeReg.of(MeanGlobalStep.MeanNumber.class, 111));
            add(GryoTypeReg.of(TreeStep.TreeBiOperator.class, 112));
            // skip 113
            add(GryoTypeReg.of(RangeGlobalStep.RangeBiOperator.class, 114));
            add(GryoTypeReg.of(OrderGlobalStep.OrderBiOperator.class, 118));
            add(GryoTypeReg.of(ProfileStep.ProfileBiOperator.class, 119));
            add(GryoTypeReg.of(IndexedTraverserSet.VertexIndexedTraverserSet.class, 173));
            add(GryoTypeReg.of(ReducingBarrierStep.NonEmittingSeed.class, 194));

            add(GryoTypeReg.of(B_LP_NL_O_P_S_SE_SL_Traverser.class, 174));
            add(GryoTypeReg.of(B_NL_O_S_SE_SL_Traverser.class, 175));
            add(GryoTypeReg.of(B_LP_NL_O_S_SE_SL_Traverser.class, 176));
            add(GryoTypeReg.of(NL_O_OB_S_SE_SL_Traverser.class, 177));
            add(GryoTypeReg.of(LP_NL_O_OB_S_SE_SL_Traverser.class, 178));
            add(GryoTypeReg.of(LP_NL_O_OB_P_S_SE_SL_Traverser.class, 179));
            add(GryoTypeReg.of(LabelledCounter.class, 180));
            add(GryoTypeReg.of(Stack.class, 181));
            add(GryoTypeReg.of(ReferenceMap.class, 182));

            add(GryoTypeReg.of(GValue.class, 199, new JavaSerializer()));
            add(GryoTypeReg.of(GType.class, 200, new JavaSerializer()));        // ***LAST ID***

            // placeholder serializers for classes that don't live here in core. this will allow them to be used if
            // present  or ignored if the class isn't available. either way the registration numbers are held as
            // placeholders so that the format stays stable
            tryAddDynamicType(this, "org.apache.tinkerpop.gremlin.util.message.RequestMessage",
                    "org.apache.tinkerpop.gremlin.util.ser.RequestMessageGryoSerializer", 167);
            tryAddDynamicType(this, "org.apache.tinkerpop.gremlin.util.message.ResponseMessage",
                    "org.apache.tinkerpop.gremlin.util.ser.ResponseMessageGryoSerializer", 169);

            tryAddDynamicType(this, "org.apache.tinkerpop.gremlin.sparql.process.traversal.strategy.SparqlStrategy",
                    null, 184);
        }};
    }

    public static List<TypeRegistration<?>> initV1Registrations() {
        return new ArrayList<TypeRegistration<?>>() {{
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
            add(GryoTypeReg.of(Types.ARRAYS_AS_LIST, 134, new UtilSerializers.ArraysAsListSerializer()));
            add(GryoTypeReg.of(BigInteger.class, 34));
            add(GryoTypeReg.of(BigDecimal.class, 35));
            add(GryoTypeReg.of(Calendar.class, 39));
            add(GryoTypeReg.of(Class.class, 41, new UtilSerializers.ClassSerializer()));
            add(GryoTypeReg.of(Class[].class, 166, new UtilSerializers.ClassArraySerializer()));
            add(GryoTypeReg.of(Collection.class, 37));
            add(GryoTypeReg.of(Collections.EMPTY_LIST.getClass(), 51));
            add(GryoTypeReg.of(Collections.EMPTY_MAP.getClass(), 52));
            add(GryoTypeReg.of(Collections.EMPTY_SET.getClass(), 53));
            add(GryoTypeReg.of(Collections.singleton(null).getClass(), 54));
            add(GryoTypeReg.of(Collections.singletonList(null).getClass(), 24));
            add(GryoTypeReg.of(Collections.singletonMap(null, null).getClass(), 23));
            add(GryoTypeReg.of(Types.COLLECTIONS_SYNCHRONIZED_MAP, 185, new UtilSerializers.SynchronizedMapSerializer()));
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
            add(GryoTypeReg.of(Types.HASH_MAP_NODE, 92));
            add(GryoTypeReg.of(Types.HASH_MAP_TREE_NODE, 170));
            add(GryoTypeReg.of(KryoSerializable.class, 36));
            add(GryoTypeReg.of(LinkedHashMap.class, 47));
            add(GryoTypeReg.of(LinkedHashSet.class, 71));
            add(GryoTypeReg.of(LinkedList.class, 116));
            add(GryoTypeReg.of(ConcurrentHashMap.class, 168));
            add(GryoTypeReg.of(ConcurrentHashMap.Entry.class, 169));
            add(GryoTypeReg.of(Types.LINKED_HASH_MAP_ENTRY_CLASS, 15));
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
            add(GryoTypeReg.of(java.sql.Timestamp.class, 161));
            add(GryoTypeReg.of(InetAddress.class, 162, new UtilSerializers.InetAddressSerializer()));
            add(GryoTypeReg.of(ByteBuffer.class, 163, new UtilSerializers.ByteBufferSerializer()));

            add(GryoTypeReg.of(ReferenceEdge.class, 81));
            add(GryoTypeReg.of(ReferenceVertexProperty.class, 82));
            add(GryoTypeReg.of(ReferenceProperty.class, 83));
            add(GryoTypeReg.of(ReferenceVertex.class, 84));
            add(GryoTypeReg.of(ReferencePath.class, 85));

            add(GryoTypeReg.of(StarGraph.class, 86, new StarGraphSerializer(Direction.BOTH, new GraphFilter())));

            add(GryoTypeReg.of(Edge.class, 65, new GryoSerializersV1.EdgeSerializer()));
            add(GryoTypeReg.of(Vertex.class, 66, new GryoSerializersV1.VertexSerializer()));
            add(GryoTypeReg.of(Property.class, 67, new GryoSerializersV1.PropertySerializer()));
            add(GryoTypeReg.of(VertexProperty.class, 68, new GryoSerializersV1.VertexPropertySerializer()));
            add(GryoTypeReg.of(Path.class, 59, new GryoSerializersV1.PathSerializer()));
            // skip 55
            add(GryoTypeReg.of(B_O_Traverser.class, 75));
            add(GryoTypeReg.of(O_Traverser.class, 76));
            add(GryoTypeReg.of(B_LP_O_P_S_SE_SL_Traverser.class, 77));
            add(GryoTypeReg.of(B_O_S_SE_SL_Traverser.class, 78));
            add(GryoTypeReg.of(B_LP_O_S_SE_SL_Traverser.class, 87));
            add(GryoTypeReg.of(O_OB_S_SE_SL_Traverser.class, 89));
            add(GryoTypeReg.of(LP_O_OB_S_SE_SL_Traverser.class, 90));
            add(GryoTypeReg.of(LP_O_OB_P_S_SE_SL_Traverser.class, 91));
            add(GryoTypeReg.of(ProjectedTraverser.class, 164));
            add(GryoTypeReg.of(DefaultRemoteTraverser.class, 123, new GryoSerializersV1.DefaultRemoteTraverserSerializer()));

            // 122 was Bytecode until it was removed for 4.x
            // 126 was Bytecode.Binding until it was removed 4.x

            add(GryoTypeReg.of(P.class, 124, new GryoSerializersV1.PSerializer()));
            add(GryoTypeReg.of(TextP.class, 186, new GryoSerializersV1.TextPSerializer()));
            add(GryoTypeReg.of(Text.RegexPredicate.class, 197));
            add(GryoTypeReg.of(Lambda.class, 125, new GryoSerializersV1.LambdaSerializer()));
            add(GryoTypeReg.of(Order.class, 127));
            add(GryoTypeReg.of(Scope.class, 128));
            add(GryoTypeReg.of(VertexProperty.Cardinality.class, 131));
            add(GryoTypeReg.of(Column.class, 132));
            add(GryoTypeReg.of(Pop.class, 133));
            add(GryoTypeReg.of(SackFunctions.Barrier.class, 135));
            add(GryoTypeReg.of(Pick.class, 137));
            add(GryoTypeReg.of(DT.class, 198));
            add(GryoTypeReg.of(Merge.class, 196));
            add(GryoTypeReg.of(HashSetSupplier.class, 136, new UtilSerializers.HashSetSupplierSerializer()));
            add(GryoTypeReg.of(MultiComparator.class, 165));

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
            add(GryoTypeReg.of(Triplet.class, 183, new UtilSerializers.TripletSerializer()));
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
            add(GryoTypeReg.of(GroupStep.GroupBiOperator.class, 117, new JavaSerializer()));
            add(GryoTypeReg.of(MeanGlobalStep.MeanGlobalBiOperator.class, 110));
            add(GryoTypeReg.of(MeanGlobalStep.MeanNumber.class, 111));
            add(GryoTypeReg.of(TreeStep.TreeBiOperator.class, 112));
            add(GryoTypeReg.of(ReducingBarrierStep.NonEmittingSeed.class, 194));

            // skip 113
            add(GryoTypeReg.of(RangeGlobalStep.RangeBiOperator.class, 114));
            add(GryoTypeReg.of(OrderGlobalStep.OrderBiOperator.class, 118, new JavaSerializer()));
            add(GryoTypeReg.of(ProfileStep.ProfileBiOperator.class, 119));

            add(GryoTypeReg.of(ConnectiveStrategy.class, 138));
            add(GryoTypeReg.of(HaltedTraverserStrategy.class, 139));
            add(GryoTypeReg.of(PartitionStrategy.class, 140, new JavaSerializer()));
            add(GryoTypeReg.of(SubgraphStrategy.class, 141, new JavaSerializer()));
            add(GryoTypeReg.of(SeedStrategy.class, 192, new JavaSerializer()));
            add(GryoTypeReg.of(VertexProgramStrategy.class, 142, new JavaSerializer()));
            add(GryoTypeReg.of(MatchAlgorithmStrategy.class, 143));
            add(GryoTypeReg.of(MatchStep.GreedyMatchAlgorithm.class, 144));
            add(GryoTypeReg.of(AdjacentToIncidentStrategy.class, 145));
            add(GryoTypeReg.of(ByModulatorOptimizationStrategy.class, 191));
            add(GryoTypeReg.of(ProductiveByStrategy.class, 195, new JavaSerializer()));
            add(GryoTypeReg.of(CountStrategy.class, 155));
            add(GryoTypeReg.of(FilterRankingStrategy.class, 146));
            add(GryoTypeReg.of(IdentityRemovalStrategy.class, 147));
            add(GryoTypeReg.of(IncidentToAdjacentStrategy.class, 148));
            add(GryoTypeReg.of(InlineFilterStrategy.class, 149));
            add(GryoTypeReg.of(LazyBarrierStrategy.class, 150));
            add(GryoTypeReg.of(MatchPredicateStrategy.class, 151));
            add(GryoTypeReg.of(OrderLimitStrategy.class, 152));
            add(GryoTypeReg.of(OptionsStrategy.class, 187, new JavaSerializer()));
            add(GryoTypeReg.of(PathProcessorStrategy.class, 153));
            add(GryoTypeReg.of(PathRetractionStrategy.class, 154));
            add(GryoTypeReg.of(RepeatUnrollStrategy.class, 156));
            add(GryoTypeReg.of(GraphFilterStrategy.class, 157));
            add(GryoTypeReg.of(LambdaRestrictionStrategy.class, 158));
            add(GryoTypeReg.of(ReadOnlyStrategy.class, 159));
            add(GryoTypeReg.of(EarlyLimitStrategy.class, 188));
            add(GryoTypeReg.of(MatchStep.CountMatchAlgorithm.class, 160));
            add(GryoTypeReg.of(MatchStep.GreedyMatchAlgorithm.class, 167));
            add(GryoTypeReg.of(EdgeLabelVerificationStrategy.class, 189));
            add(GryoTypeReg.of(ReservedKeysVerificationStrategy.class, 190));
            // skip 171, 172 to sync with the 3.3.x
            add(GryoTypeReg.of(IndexedTraverserSet.VertexIndexedTraverserSet.class, 173));

            add(GryoTypeReg.of(B_LP_NL_O_P_S_SE_SL_Traverser.class, 174));
            add(GryoTypeReg.of(B_NL_O_S_SE_SL_Traverser.class, 175));
            add(GryoTypeReg.of(B_LP_NL_O_S_SE_SL_Traverser.class, 176));
            add(GryoTypeReg.of(NL_O_OB_S_SE_SL_Traverser.class, 177));
            add(GryoTypeReg.of(LP_NL_O_OB_S_SE_SL_Traverser.class, 178));
            add(GryoTypeReg.of(LP_NL_O_OB_P_S_SE_SL_Traverser.class, 179));
            add(GryoTypeReg.of(LabelledCounter.class, 180));
            add(GryoTypeReg.of(Stack.class, 181));
            add(GryoTypeReg.of(ReferenceMap.class, 182));

            add(GryoTypeReg.of(GValue.class, 199, new JavaSerializer()));
            add(GryoTypeReg.of(GType.class, 200, new JavaSerializer()));        // ***LAST ID***
        }};
    }

    private static void tryAddDynamicType(final List<TypeRegistration<?>> types, final String type,
                                            final String serializer, final int registrationId) {
        try {
            final Class typeClass = Class.forName(type);
            final Optional<SerializerShim<?>> serializerInstance = Optional.ofNullable(serializer)
                    .map(FunctionUtils.wrapFunction(Class::forName))
                    .map(FunctionUtils.wrapFunction(c -> (SerializerShim<?>) c.getConstructor().newInstance()));
            if (serializerInstance.isPresent()) {
                types.add(GryoTypeReg.of(typeClass, registrationId, serializerInstance.get()));
            } else {
                types.add(GryoTypeReg.of(typeClass, registrationId));
            }
        } catch (Exception ignored) {
            // if the class isn't here - no worries
        }
    }

    private static final class Types {
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

        private static final Class HASH_MAP_TREE_NODE;

        private static final Class COLLECTIONS_SYNCHRONIZED_MAP;

        static {
            // have to instantiate this via reflection because it is a private inner class of HashMap
            String className = HashMap.class.getName() + "$Node";
            try {
                HASH_MAP_NODE = Class.forName(className);
            } catch (Exception ex) {
                throw new RuntimeException("Could not access " + className, ex);
            }

            className = HashMap.class.getName() + "$TreeNode";
            try {
                HASH_MAP_TREE_NODE = Class.forName(className);
            } catch (Exception ex) {
                throw new RuntimeException("Could not access " + className, ex);
            }

            className = Collections.class.getName() + "$SynchronizedMap";
            try {
                COLLECTIONS_SYNCHRONIZED_MAP = Class.forName(className);
            } catch (Exception ex) {
                throw new RuntimeException("Could not access " + className, ex);
            }
        }

        private Types() {}
    }
}
