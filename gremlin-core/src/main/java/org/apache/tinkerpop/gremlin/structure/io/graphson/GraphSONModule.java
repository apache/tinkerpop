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

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
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
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
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
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.DirectionalStarGraph;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializerV1;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializerV2;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializerV3;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializerV4;
import org.apache.tinkerpop.gremlin.util.function.Lambda;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The set of serializers that handle the core graph interfaces.  These serializers support normalization which
 * ensures that generated GraphSON will be compatible with line-based versioning tools. This setting comes with
 * some overhead, with respect to key sorting and other in-memory operations.
 * <p/>
 * This is a base class for grouping these core serializers.  Concrete extensions of this class represent a "version"
 * that should be registered with the {@link GraphSONVersion} enum.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
abstract class GraphSONModule extends TinkerPopJacksonModule {

    GraphSONModule(final String name) {
        super(name);
    }

    /**
     * Attempt to load {@code SparqlStrategy} if it's on the path. Dynamically loading it from core makes it easier
     * for users as they won't have to register special modules for serialization purposes.
     */
    private static Optional<Class<?>> tryLoadSparqlStrategy() {
        try {
            return Optional.of(Class.forName("org.apache.tinkerpop.gremlin.sparql.process.traversal.strategy.SparqlStrategy"));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    /**
     * Version 4.0 of GraphSON.
     */
    static final class GraphSONModuleV4 extends GraphSONModule {

        private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
                new LinkedHashMap<Class, String>() {{
                    // Those don't have deserializers because handled by Jackson,
                    // but we still want to rename them in GraphSON
                    put(Integer.class, "Int32");
                    put(Long.class, "Int64");
                    put(Double.class, "Double");
                    put(Float.class, "Float");

                    // BulkSet is expanded to List during serialization but we want the List deserializer to be
                    // registered to g:List so this entry must be added before List.
                    put(BulkSet.class, "List");

                    put(Map.class, "Map");
                    put(List.class, "List");
                    put(Set.class, "Set");

                    // TinkerPop Graph objects
                    put(Vertex.class, "Vertex");
                    put(Edge.class, "Edge");
                    put(Property.class, "Property");
                    put(Path.class, "Path");
                    put(VertexProperty.class, "VertexProperty");
                    put(Tree.class, "Tree");
                    Stream.of(
                            Direction.class,
                            Merge.class,
                            T.class).forEach(e -> put(e, e.getSimpleName()));
                }});

        /**
         * Constructs a new object.
         */
        protected GraphSONModuleV4(final boolean normalize, final TypeInfo typeInfo) {
            super("graphson-4.0");

            /////////////////////// SERIALIZERS ////////////////////////////

            // graph
            addSerializer(Edge.class, new GraphSONSerializersV4.EdgeJacksonSerializer(normalize, typeInfo));
            addSerializer(Vertex.class, new GraphSONSerializersV4.VertexJacksonSerializer(normalize, typeInfo));
            addSerializer(VertexProperty.class, new GraphSONSerializersV4.VertexPropertyJacksonSerializer(normalize, true));
            addSerializer(Property.class, new GraphSONSerializersV4.PropertyJacksonSerializer());
            addSerializer(Path.class, new GraphSONSerializersV4.PathJacksonSerializer());
            addSerializer(DirectionalStarGraph.class, new StarGraphGraphSONSerializerV4(normalize));
            addSerializer(Tree.class, new GraphSONSerializersV4.TreeJacksonSerializer());

            // java.util - use the standard jackson serializers for collections when types aren't embedded
            if (typeInfo != TypeInfo.NO_TYPES) {
                addSerializer(Map.Entry.class, new JavaUtilSerializersV4.MapEntryJacksonSerializer());
                addSerializer(Map.class, new JavaUtilSerializersV4.MapJacksonSerializer());
                addSerializer(List.class, new JavaUtilSerializersV4.ListJacksonSerializer());
                addSerializer(Set.class, new JavaUtilSerializersV4.SetJacksonSerializer());
            }

            // need to explicitly add serializers for these types because Jackson doesn't do it at all.
            addSerializer(Integer.class, new GraphSONSerializersV4.IntegerGraphSONSerializer());
            addSerializer(Double.class, new GraphSONSerializersV4.DoubleGraphSONSerializer());

            // traversal
            addSerializer(BulkSet.class, new TraversalSerializersV4.BulkSetJacksonSerializer());
            Stream.of(
                    Direction.class,
                    Merge.class,
                    T.class).forEach(e -> addSerializer(e, new TraversalSerializersV4.EnumJacksonSerializer()));

            /////////////////////// DESERIALIZERS ////////////////////////////

            // Tinkerpop Graph
            addDeserializer(Vertex.class, new GraphSONSerializersV4.VertexJacksonDeserializer());
            addDeserializer(Edge.class, new GraphSONSerializersV4.EdgeJacksonDeserializer());
            addDeserializer(Property.class, new GraphSONSerializersV4.PropertyJacksonDeserializer());
            addDeserializer(Path.class, new GraphSONSerializersV4.PathJacksonDeserializer());
            addDeserializer(VertexProperty.class, new GraphSONSerializersV4.VertexPropertyJacksonDeserializer());
            addDeserializer(Tree.class, new GraphSONSerializersV4.TreeJacksonDeserializer());

            // java.util - use the standard jackson serializers for collections when types aren't embedded
            if (typeInfo != TypeInfo.NO_TYPES) {
                addDeserializer(Map.class, new JavaUtilSerializersV4.MapJacksonDeserializer());
                addDeserializer(List.class, new JavaUtilSerializersV4.ListJacksonDeserializer());
                addDeserializer(Set.class, new JavaUtilSerializersV4.SetJacksonDeserializer());
            }

            // numbers
            addDeserializer(Integer.class, new GraphSONSerializersV4.IntegerJackonsDeserializer());
            addDeserializer(Double.class, new GraphSONSerializersV4.DoubleJacksonDeserializer());

            // traversal
            Stream.of(
                    Direction.values(),
                    T.values()).flatMap(Stream::of).forEach(e -> addDeserializer(e.getClass(), new TraversalSerializersV4.EnumJacksonDeserializer(e.getDeclaringClass())));
        }

        public static Builder build() {
            return new Builder();
        }

        @Override
        public Map<Class, String> getTypeDefinitions() {
            return TYPE_DEFINITIONS;
        }

        @Override
        public String getTypeNamespace() {
            return GraphSONTokens.GREMLIN_TYPE_NAMESPACE;
        }

        static final class Builder implements GraphSONModuleBuilder {

            private Builder() {
            }

            @Override
            public GraphSONModule create(final boolean normalize, final TypeInfo typeInfo) {
                return new GraphSONModuleV4(normalize, typeInfo);
            }

        }
    }

    /**
     * Version 3.0 of GraphSON.
     */
    static final class GraphSONModuleV3 extends GraphSONModule {

        private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
                new LinkedHashMap<Class, String>() {{
                    // Those don't have deserializers because handled by Jackson,
                    // but we still want to rename them in GraphSON
                    put(Integer.class, "Int32");
                    put(Long.class, "Int64");
                    put(Double.class, "Double");
                    put(Float.class, "Float");

                    put(Map.class, "Map");
                    put(List.class, "List");
                    put(Set.class, "Set");

                    // TinkerPop Graph objects
                    put(Lambda.class, "Lambda");
                    put(Vertex.class, "Vertex");
                    put(Edge.class, "Edge");
                    put(Property.class, "Property");
                    put(Path.class, "Path");
                    put(VertexProperty.class, "VertexProperty");
                    put(Metrics.class, "Metrics");
                    put(TraversalMetrics.class, "TraversalMetrics");
                    put(TraversalExplanation.class, "TraversalExplanation");
                    put(Traverser.class, "Traverser");
                    put(Tree.class, "Tree");
                    put(BulkSet.class, "BulkSet");
                    put(AndP.class, "P");
                    put(OrP.class, "P");
                    put(P.class, "P");
                    put(TextP.class, "TextP");
                    Stream.of(
                            VertexProperty.Cardinality.class,
                            Column.class,
                            Direction.class,
                            DT.class,
                            Merge.class,
                            Operator.class,
                            Order.class,
                            Pop.class,
                            SackFunctions.Barrier.class,
                            Pick.class,
                            Scope.class,
                            T.class).forEach(e -> put(e, e.getSimpleName()));
                    Arrays.asList(
                            ConnectiveStrategy.class,
                            ElementIdStrategy.class,
                            EventStrategy.class,
                            HaltedTraverserStrategy.class,
                            PartitionStrategy.class,
                            SubgraphStrategy.class,
                            SeedStrategy.class,
                            LazyBarrierStrategy.class,
                            MatchAlgorithmStrategy.class,
                            AdjacentToIncidentStrategy.class,
                            ByModulatorOptimizationStrategy.class,
                            ProductiveByStrategy.class,
                            CountStrategy.class,
                            FilterRankingStrategy.class,
                            IdentityRemovalStrategy.class,
                            IncidentToAdjacentStrategy.class,
                            InlineFilterStrategy.class,
                            MatchPredicateStrategy.class,
                            OrderLimitStrategy.class,
                            OptionsStrategy.class,
                            PathProcessorStrategy.class,
                            PathRetractionStrategy.class,
                            RepeatUnrollStrategy.class,
                            ComputerVerificationStrategy.class,
                            LambdaRestrictionStrategy.class,
                            ReadOnlyStrategy.class,
                            StandardVerificationStrategy.class,
                            EarlyLimitStrategy.class,
                            EdgeLabelVerificationStrategy.class,
                            ReservedKeysVerificationStrategy.class,
                            //
                            GraphFilterStrategy.class,
                            VertexProgramStrategy.class
                    ).forEach(strategy -> put(strategy, strategy.getSimpleName()));

                    GraphSONModule.tryLoadSparqlStrategy().ifPresent(s -> put(s, s.getSimpleName()));
                }});

        /**
         * Constructs a new object.
         */
        protected GraphSONModuleV3(final boolean normalize, final TypeInfo typeInfo) {
            super("graphson-3.0");

            /////////////////////// SERIALIZERS ////////////////////////////

            // graph
            addSerializer(Edge.class, new GraphSONSerializersV3.EdgeJacksonSerializer(normalize, typeInfo));
            addSerializer(Vertex.class, new GraphSONSerializersV3.VertexJacksonSerializer(normalize, typeInfo));
            addSerializer(VertexProperty.class, new GraphSONSerializersV3.VertexPropertyJacksonSerializer(normalize, true));
            addSerializer(Property.class, new GraphSONSerializersV3.PropertyJacksonSerializer());
            addSerializer(Metrics.class, new GraphSONSerializersV3.MetricsJacksonSerializer());
            addSerializer(TraversalMetrics.class, new GraphSONSerializersV3.TraversalMetricsJacksonSerializer());
            addSerializer(TraversalExplanation.class, new GraphSONSerializersV3.TraversalExplanationJacksonSerializer());
            addSerializer(Path.class, new GraphSONSerializersV3.PathJacksonSerializer());
            addSerializer(DirectionalStarGraph.class, new StarGraphGraphSONSerializerV3(normalize));
            addSerializer(Tree.class, new GraphSONSerializersV3.TreeJacksonSerializer());

            // java.util - use the standard jackson serializers for collections when types aren't embedded
            if (typeInfo != TypeInfo.NO_TYPES) {
                addSerializer(Map.Entry.class, new JavaUtilSerializersV3.MapEntryJacksonSerializer());
                addSerializer(Map.class, new JavaUtilSerializersV3.MapJacksonSerializer());
                addSerializer(List.class, new JavaUtilSerializersV3.ListJacksonSerializer());
                addSerializer(Set.class, new JavaUtilSerializersV3.SetJacksonSerializer());
            }

            // need to explicitly add serializers for these types because Jackson doesn't do it at all.
            addSerializer(Integer.class, new GraphSONSerializersV3.IntegerGraphSONSerializer());
            addSerializer(Double.class, new GraphSONSerializersV3.DoubleGraphSONSerializer());

            // traversal
            addSerializer(BulkSet.class, new TraversalSerializersV3.BulkSetJacksonSerializer());
            addSerializer(Traversal.class, new TraversalSerializersV3.TraversalJacksonSerializer());
            Stream.of(VertexProperty.Cardinality.class,
                    Column.class,
                    Direction.class,
                    DT.class,
                    Merge.class,
                    Operator.class,
                    Order.class,
                    Pop.class,
                    SackFunctions.Barrier.class,
                    Scope.class,
                    Pick.class,
                    T.class).forEach(e -> addSerializer(e, new TraversalSerializersV3.EnumJacksonSerializer()));
            addSerializer(P.class, new TraversalSerializersV3.PJacksonSerializer());
            addSerializer(Lambda.class, new TraversalSerializersV3.LambdaJacksonSerializer());
            addSerializer(Traverser.class, new TraversalSerializersV3.TraverserJacksonSerializer());
            addSerializer(TraversalStrategy.class, new TraversalSerializersV3.TraversalStrategyJacksonSerializer());

            /////////////////////// DESERIALIZERS ////////////////////////////

            // Tinkerpop Graph
            addDeserializer(Vertex.class, new GraphSONSerializersV3.VertexJacksonDeserializer());
            addDeserializer(Edge.class, new GraphSONSerializersV3.EdgeJacksonDeserializer());
            addDeserializer(Property.class, new GraphSONSerializersV3.PropertyJacksonDeserializer());
            addDeserializer(Path.class, new GraphSONSerializersV3.PathJacksonDeserializer());
            addDeserializer(TraversalExplanation.class, new GraphSONSerializersV3.TraversalExplanationJacksonDeserializer());
            addDeserializer(VertexProperty.class, new GraphSONSerializersV3.VertexPropertyJacksonDeserializer());
            addDeserializer(Metrics.class, new GraphSONSerializersV3.MetricsJacksonDeserializer());
            addDeserializer(TraversalMetrics.class, new GraphSONSerializersV3.TraversalMetricsJacksonDeserializer());
            addDeserializer(Tree.class, new GraphSONSerializersV3.TreeJacksonDeserializer());

            // java.util - use the standard jackson serializers for collections when types aren't embedded
            if (typeInfo != TypeInfo.NO_TYPES) {
                addDeserializer(Map.class, new JavaUtilSerializersV3.MapJacksonDeserializer());
                addDeserializer(List.class, new JavaUtilSerializersV3.ListJacksonDeserializer());
                addDeserializer(Set.class, new JavaUtilSerializersV3.SetJacksonDeserializer());
            }

            // numbers
            addDeserializer(Integer.class, new GraphSONSerializersV3.IntegerJackonsDeserializer());
            addDeserializer(Double.class, new GraphSONSerializersV3.DoubleJacksonDeserializer());

            // traversal
            addDeserializer(BulkSet.class, new TraversalSerializersV3.BulkSetJacksonDeserializer());
            Stream.of(VertexProperty.Cardinality.values(),
                    Column.values(),
                    Direction.values(),
                    DT.values(),
                    Merge.values(),
                    Operator.values(),
                    Order.values(),
                    Pop.values(),
                    SackFunctions.Barrier.values(),
                    Scope.values(),
                    Pick.values(),
                    T.values()).flatMap(Stream::of).forEach(e -> addDeserializer(e.getClass(), new TraversalSerializersV3.EnumJacksonDeserializer(e.getDeclaringClass())));
            addDeserializer(P.class, new TraversalSerializersV3.PJacksonDeserializer());
            addDeserializer(TextP.class, new TraversalSerializersV3.TextPJacksonDeserializer());
            addDeserializer(Lambda.class, new TraversalSerializersV3.LambdaJacksonDeserializer());
            addDeserializer(Traverser.class, new TraversalSerializersV3.TraverserJacksonDeserializer());
            Arrays.asList(
                    ConnectiveStrategy.class,
                    ElementIdStrategy.class,
                    EventStrategy.class,
                    HaltedTraverserStrategy.class,
                    PartitionStrategy.class,
                    SubgraphStrategy.class,
                    SeedStrategy.class,
                    LazyBarrierStrategy.class,
                    MatchAlgorithmStrategy.class,
                    AdjacentToIncidentStrategy.class,
                    ByModulatorOptimizationStrategy.class,
                    ProductiveByStrategy.class,
                    CountStrategy.class,
                    FilterRankingStrategy.class,
                    IdentityRemovalStrategy.class,
                    IncidentToAdjacentStrategy.class,
                    InlineFilterStrategy.class,
                    MatchPredicateStrategy.class,
                    OrderLimitStrategy.class,
                    OptionsStrategy.class,
                    PathProcessorStrategy.class,
                    PathRetractionStrategy.class,
                    RepeatUnrollStrategy.class,
                    ComputerVerificationStrategy.class,
                    LambdaRestrictionStrategy.class,
                    ReadOnlyStrategy.class,
                    StandardVerificationStrategy.class,
                    EarlyLimitStrategy.class,
                    EdgeLabelVerificationStrategy.class,
                    ReservedKeysVerificationStrategy.class,
                    //
                    GraphFilterStrategy.class,
                    VertexProgramStrategy.class
            ).forEach(strategy -> addDeserializer(strategy, new TraversalSerializersV3.TraversalStrategyProxyJacksonDeserializer(strategy)));

            GraphSONModule.tryLoadSparqlStrategy().ifPresent(s -> addDeserializer(s, new TraversalSerializersV3.TraversalStrategyProxyJacksonDeserializer(s)));
        }

        public static Builder build() {
            return new Builder();
        }

        @Override
        public Map<Class, String> getTypeDefinitions() {
            return TYPE_DEFINITIONS;
        }

        @Override
        public String getTypeNamespace() {
            return GraphSONTokens.GREMLIN_TYPE_NAMESPACE;
        }

        static final class Builder implements GraphSONModuleBuilder {

            private Builder() {
            }

            @Override
            public GraphSONModule create(final boolean normalize, final TypeInfo typeInfo) {
                return new GraphSONModuleV3(normalize, typeInfo);
            }

        }
    }

    /**
     * Version 2.0 of GraphSON.
     */
    static final class GraphSONModuleV2 extends GraphSONModule {

        private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
                new LinkedHashMap<Class, String>() {{
                    // Those don't have deserializers because handled by Jackson,
                    // but we still want to rename them in GraphSON
                    put(Integer.class, "Int32");
                    put(Long.class, "Int64");
                    put(Double.class, "Double");
                    put(Float.class, "Float");

                    // Tinkerpop Graph objects
                    put(Lambda.class, "Lambda");
                    put(Vertex.class, "Vertex");
                    put(Edge.class, "Edge");
                    put(Property.class, "Property");
                    put(Path.class, "Path");
                    put(VertexProperty.class, "VertexProperty");
                    put(Metrics.class, "Metrics");
                    put(TraversalMetrics.class, "TraversalMetrics");
                    put(TraversalExplanation.class, "TraversalExplanation");
                    put(Traverser.class, "Traverser");
                    put(Tree.class, "Tree");
                    put(AndP.class, "P");
                    put(OrP.class, "P");
                    put(P.class, "P");
                    put(TextP.class, "TextP");
                    Stream.of(
                            VertexProperty.Cardinality.class,
                            Column.class,
                            Direction.class,
                            DT.class,
                            Merge.class,
                            Operator.class,
                            Order.class,
                            Pop.class,
                            SackFunctions.Barrier.class,
                            Pick.class,
                            Scope.class,
                            T.class).forEach(e -> put(e, e.getSimpleName()));
                    Arrays.asList(
                            ConnectiveStrategy.class,
                            ElementIdStrategy.class,
                            EventStrategy.class,
                            HaltedTraverserStrategy.class,
                            PartitionStrategy.class,
                            SubgraphStrategy.class,
                            SeedStrategy.class,
                            LazyBarrierStrategy.class,
                            MatchAlgorithmStrategy.class,
                            AdjacentToIncidentStrategy.class,
                            ByModulatorOptimizationStrategy.class,
                            ProductiveByStrategy.class,
                            CountStrategy.class,
                            FilterRankingStrategy.class,
                            IdentityRemovalStrategy.class,
                            IncidentToAdjacentStrategy.class,
                            InlineFilterStrategy.class,
                            MatchPredicateStrategy.class,
                            OrderLimitStrategy.class,
                            OptionsStrategy.class,
                            PathProcessorStrategy.class,
                            PathRetractionStrategy.class,
                            RepeatUnrollStrategy.class,
                            ComputerVerificationStrategy.class,
                            LambdaRestrictionStrategy.class,
                            ReadOnlyStrategy.class,
                            StandardVerificationStrategy.class,
                            EarlyLimitStrategy.class,
                            EdgeLabelVerificationStrategy.class,
                            ReservedKeysVerificationStrategy.class,
                            //
                            GraphFilterStrategy.class,
                            VertexProgramStrategy.class
                    ).forEach(strategy -> put(strategy, strategy.getSimpleName()));

                    GraphSONModule.tryLoadSparqlStrategy().ifPresent(s -> put(s, s.getSimpleName()));
                }});

        /**
         * Constructs a new object.
         */
        protected GraphSONModuleV2(final boolean normalize) {
            super("graphson-2.0");

            /////////////////////// SERIALIZERS ////////////////////////////

            // graph
            addSerializer(Edge.class, new GraphSONSerializersV2.EdgeJacksonSerializer(normalize));
            addSerializer(Vertex.class, new GraphSONSerializersV2.VertexJacksonSerializer(normalize));
            addSerializer(VertexProperty.class, new GraphSONSerializersV2.VertexPropertyJacksonSerializer(normalize, true));
            addSerializer(Property.class, new GraphSONSerializersV2.PropertyJacksonSerializer());
            addSerializer(Metrics.class, new GraphSONSerializersV2.MetricsJacksonSerializer());
            addSerializer(TraversalMetrics.class, new GraphSONSerializersV2.TraversalMetricsJacksonSerializer());
            addSerializer(TraversalExplanation.class, new GraphSONSerializersV2.TraversalExplanationJacksonSerializer());
            addSerializer(Path.class, new GraphSONSerializersV2.PathJacksonSerializer());
            addSerializer(DirectionalStarGraph.class, new StarGraphGraphSONSerializerV2(normalize));
            addSerializer(Tree.class, new GraphSONSerializersV2.TreeJacksonSerializer());

            // java.util
            addSerializer(Map.Entry.class, new JavaUtilSerializersV2.MapEntryJacksonSerializer());

            // need to explicitly add serializers for those types because Jackson doesn't do it at all.
            addSerializer(Integer.class, new GraphSONSerializersV2.IntegerGraphSONSerializer());
            addSerializer(Double.class, new GraphSONSerializersV2.DoubleGraphSONSerializer());

            // traversal
            addSerializer(Traversal.class, new TraversalSerializersV2.TraversalJacksonSerializer());
            Stream.of(VertexProperty.Cardinality.class,
                    Column.class,
                    Direction.class,
                    DT.class,
                    Merge.class,
                    Operator.class,
                    Order.class,
                    Pop.class,
                    SackFunctions.Barrier.class,
                    Scope.class,
                    Pick.class,
                    T.class).forEach(e -> addSerializer(e, new TraversalSerializersV2.EnumJacksonSerializer()));
            addSerializer(P.class, new TraversalSerializersV2.PJacksonSerializer());
            addSerializer(Lambda.class, new TraversalSerializersV2.LambdaJacksonSerializer());
            addSerializer(Traverser.class, new TraversalSerializersV2.TraverserJacksonSerializer());
            addSerializer(TraversalStrategy.class, new TraversalSerializersV2.TraversalStrategyJacksonSerializer());

            /////////////////////// DESERIALIZERS ////////////////////////////

            // Tinkerpop Graph
            addDeserializer(Vertex.class, new GraphSONSerializersV2.VertexJacksonDeserializer());
            addDeserializer(Edge.class, new GraphSONSerializersV2.EdgeJacksonDeserializer());
            addDeserializer(Property.class, new GraphSONSerializersV2.PropertyJacksonDeserializer());
            addDeserializer(Path.class, new GraphSONSerializersV2.PathJacksonDeserializer());
            addDeserializer(VertexProperty.class, new GraphSONSerializersV2.VertexPropertyJacksonDeserializer());
            addDeserializer(TraversalExplanation.class, new GraphSONSerializersV2.TraversalExplanationJacksonDeserializer());
            addDeserializer(Metrics.class, new GraphSONSerializersV2.MetricsJacksonDeserializer());
            addDeserializer(TraversalMetrics.class, new GraphSONSerializersV2.TraversalMetricsJacksonDeserializer());
            addDeserializer(Tree.class, new GraphSONSerializersV2.TreeJacksonDeserializer());

            // numbers
            addDeserializer(Integer.class, new GraphSONSerializersV2.IntegerJacksonDeserializer());
            addDeserializer(Double.class, new GraphSONSerializersV2.DoubleJacksonDeserializer());

            // traversal
            Stream.of(VertexProperty.Cardinality.values(),
                    Column.values(),
                    Direction.values(),
                    DT.values(),
                    Merge.values(),
                    Operator.values(),
                    Order.values(),
                    Pop.values(),
                    SackFunctions.Barrier.values(),
                    Scope.values(),
                    Pick.values(),
                    T.values()).flatMap(Stream::of).forEach(e -> addDeserializer(e.getClass(), new TraversalSerializersV2.EnumJacksonDeserializer(e.getDeclaringClass())));
            addDeserializer(P.class, new TraversalSerializersV2.PJacksonDeserializer());
            addDeserializer(TextP.class, new TraversalSerializersV2.TextPJacksonDeserializer());
            addDeserializer(Lambda.class, new TraversalSerializersV2.LambdaJacksonDeserializer());
            addDeserializer(Traverser.class, new TraversalSerializersV2.TraverserJacksonDeserializer());
            Arrays.asList(
                    ConnectiveStrategy.class,
                    ElementIdStrategy.class,
                    EventStrategy.class,
                    HaltedTraverserStrategy.class,
                    PartitionStrategy.class,
                    SubgraphStrategy.class,
                    SeedStrategy.class,
                    LazyBarrierStrategy.class,
                    MatchAlgorithmStrategy.class,
                    AdjacentToIncidentStrategy.class,
                    ByModulatorOptimizationStrategy.class,
                    ProductiveByStrategy.class,
                    CountStrategy.class,
                    FilterRankingStrategy.class,
                    IdentityRemovalStrategy.class,
                    IncidentToAdjacentStrategy.class,
                    InlineFilterStrategy.class,
                    MatchPredicateStrategy.class,
                    OrderLimitStrategy.class,
                    OptionsStrategy.class,
                    PathProcessorStrategy.class,
                    PathRetractionStrategy.class,
                    RepeatUnrollStrategy.class,
                    ComputerVerificationStrategy.class,
                    LambdaRestrictionStrategy.class,
                    ReadOnlyStrategy.class,
                    StandardVerificationStrategy.class,
                    EarlyLimitStrategy.class,
                    EdgeLabelVerificationStrategy.class,
                    ReservedKeysVerificationStrategy.class,
                    //
                    GraphFilterStrategy.class,
                    VertexProgramStrategy.class
            ).forEach(strategy -> addDeserializer(strategy, new TraversalSerializersV2.TraversalStrategyProxyJacksonDeserializer(strategy)));

            GraphSONModule.tryLoadSparqlStrategy().ifPresent(s -> addDeserializer(s, new TraversalSerializersV2.TraversalStrategyProxyJacksonDeserializer(s)));
        }

        public static Builder build() {
            return new Builder();
        }

        @Override
        public Map<Class, String> getTypeDefinitions() {
            return TYPE_DEFINITIONS;
        }

        @Override
        public String getTypeNamespace() {
            return GraphSONTokens.GREMLIN_TYPE_NAMESPACE;
        }

        static final class Builder implements GraphSONModuleBuilder {

            private Builder() {
            }

            @Override
            public GraphSONModule create(final boolean normalize, final TypeInfo typeInfo) {
                return new GraphSONModuleV2(normalize);
            }

        }
    }

    /**
     * Version 1.0 of GraphSON.
     */
    static final class GraphSONModuleV1 extends GraphSONModule {

        /**
         * Constructs a new object.
         */
        protected GraphSONModuleV1(final boolean normalize) {
            super("graphson-1.0");
            // graph
            addSerializer(Edge.class, new GraphSONSerializersV1.EdgeJacksonSerializer(normalize));
            addSerializer(Vertex.class, new GraphSONSerializersV1.VertexJacksonSerializer(normalize));
            addSerializer(VertexProperty.class, new GraphSONSerializersV1.VertexPropertyJacksonSerializer(normalize));
            addSerializer(Property.class, new GraphSONSerializersV1.PropertyJacksonSerializer());
            addSerializer(TraversalMetrics.class, new GraphSONSerializersV1.TraversalMetricsJacksonSerializer());
            addSerializer(TraversalExplanation.class, new GraphSONSerializersV1.TraversalExplanationJacksonSerializer());
            addSerializer(Path.class, new GraphSONSerializersV1.PathJacksonSerializer());
            addSerializer(DirectionalStarGraph.class, new StarGraphGraphSONSerializerV1(normalize));
            addSerializer(Tree.class, new GraphSONSerializersV1.TreeJacksonSerializer());

            // java.util
            addSerializer(Map.Entry.class, new JavaUtilSerializersV1.MapEntryJacksonSerializer());

            // java.time
            addSerializer(Duration.class, new JavaTimeSerializersV1.DurationJacksonSerializer());
            addSerializer(Instant.class, new JavaTimeSerializersV1.InstantJacksonSerializer());
            addSerializer(LocalDate.class, new JavaTimeSerializersV1.LocalDateJacksonSerializer());
            addSerializer(LocalDateTime.class, new JavaTimeSerializersV1.LocalDateTimeJacksonSerializer());
            addSerializer(LocalTime.class, new JavaTimeSerializersV1.LocalTimeJacksonSerializer());
            addSerializer(MonthDay.class, new JavaTimeSerializersV1.MonthDayJacksonSerializer());
            addSerializer(OffsetDateTime.class, new JavaTimeSerializersV1.OffsetDateTimeJacksonSerializer());
            addSerializer(OffsetTime.class, new JavaTimeSerializersV1.OffsetTimeJacksonSerializer());
            addSerializer(Period.class, new JavaTimeSerializersV1.PeriodJacksonSerializer());
            addSerializer(Year.class, new JavaTimeSerializersV1.YearJacksonSerializer());
            addSerializer(YearMonth.class, new JavaTimeSerializersV1.YearMonthJacksonSerializer());
            addSerializer(ZonedDateTime.class, new JavaTimeSerializersV1.ZonedDateTimeJacksonSerializer());
            addSerializer(ZoneOffset.class, new JavaTimeSerializersV1.ZoneOffsetJacksonSerializer());

            addDeserializer(Duration.class, new JavaTimeSerializersV1.DurationJacksonDeserializer());
            addDeserializer(Instant.class, new JavaTimeSerializersV1.InstantJacksonDeserializer());
            addDeserializer(LocalDate.class, new JavaTimeSerializersV1.LocalDateJacksonDeserializer());
            addDeserializer(LocalDateTime.class, new JavaTimeSerializersV1.LocalDateTimeJacksonDeserializer());
            addDeserializer(LocalTime.class, new JavaTimeSerializersV1.LocalTimeJacksonDeserializer());
            addDeserializer(MonthDay.class, new JavaTimeSerializersV1.MonthDayJacksonDeserializer());
            addDeserializer(OffsetDateTime.class, new JavaTimeSerializersV1.OffsetDateTimeJacksonDeserializer());
            addDeserializer(OffsetTime.class, new JavaTimeSerializersV1.OffsetTimeJacksonDeserializer());
            addDeserializer(Period.class, new JavaTimeSerializersV1.PeriodJacksonDeserializer());
            addDeserializer(Year.class, new JavaTimeSerializersV1.YearJacksonDeserializer());
            addDeserializer(YearMonth.class, new JavaTimeSerializersV1.YearMonthJacksonDeserializer());
            addDeserializer(ZonedDateTime.class, new JavaTimeSerializersV1.ZonedDateTimeJacksonDeserializer());
            addDeserializer(ZoneOffset.class, new JavaTimeSerializersV1.ZoneOffsetJacksonDeserializer());
        }

        public static Builder build() {
            return new Builder();
        }

        @Override
        public Map<Class, String> getTypeDefinitions() {
            // null is fine and handled by the GraphSONMapper
            return null;
        }

        @Override
        public String getTypeNamespace() {
            // null is fine and handled by the GraphSONMapper
            return null;
        }

        static final class Builder implements GraphSONModuleBuilder {

            private Builder() {
            }

            @Override
            public GraphSONModule create(final boolean normalize, final TypeInfo typeInfo) {
                return new GraphSONModuleV1(normalize);
            }
        }
    }

    /**
     * A "builder" used to create {@link GraphSONModule} instances.  Each "version" should have an associated
     * {@code GraphSONModuleBuilder} so that it can be registered with the {@link GraphSONVersion} enum.
     */
    static interface GraphSONModuleBuilder {

        /**
         * Creates a new {@link GraphSONModule} object.
         *
         * @param normalize when set to true, keys and objects are ordered to ensure that they occur in
         *                  the same order.
         * @param typeInfo allows the module to react to the specified typeinfo given to the mapper
         */
        GraphSONModule create(final boolean normalize, final TypeInfo typeInfo);
    }
}
