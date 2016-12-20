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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.OrderLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathProcessorStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RangeByIsCountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
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
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializerV1d0;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializerV2d0;
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
import java.util.Map;
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
     * Version 2.0 of GraphSON.
     */
    static final class GraphSONModuleV2d0 extends GraphSONModule {

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
                    put(Traverser.class, "Traverser");
                    put(Tree.class, "Tree");
                    put(Bytecode.class, "Bytecode");
                    put(Bytecode.Binding.class, "Binding");
                    put(AndP.class, "P");
                    put(OrP.class, "P");
                    put(P.class, "P");
                    Stream.of(
                            VertexProperty.Cardinality.values(),
                            Column.values(),
                            Direction.values(),
                            Operator.values(),
                            Order.values(),
                            Pop.values(),
                            SackFunctions.Barrier.values(),
                            TraversalOptionParent.Pick.values(),
                            Scope.values(),
                            T.values()).flatMap(Stream::of).forEach(e -> put(e.getClass(), e.getDeclaringClass().getSimpleName()));
                    Arrays.asList(
                            ConnectiveStrategy.class,
                            ElementIdStrategy.class,
                            EventStrategy.class,
                            HaltedTraverserStrategy.class,
                            PartitionStrategy.class,
                            SubgraphStrategy.class,
                            LazyBarrierStrategy.class,
                            MatchAlgorithmStrategy.class,
                            AdjacentToIncidentStrategy.class,
                            FilterRankingStrategy.class,
                            IdentityRemovalStrategy.class,
                            IncidentToAdjacentStrategy.class,
                            InlineFilterStrategy.class,
                            MatchPredicateStrategy.class,
                            OrderLimitStrategy.class,
                            PathProcessorStrategy.class,
                            PathRetractionStrategy.class,
                            RangeByIsCountStrategy.class,
                            RepeatUnrollStrategy.class,
                            ComputerVerificationStrategy.class,
                            LambdaRestrictionStrategy.class,
                            ReadOnlyStrategy.class,
                            StandardVerificationStrategy.class,
                            //
                            GraphFilterStrategy.class,
                            VertexProgramStrategy.class
                    ).forEach(strategy -> put(strategy, strategy.getSimpleName()));
                }});

        /**
         * Constructs a new object.
         */
        protected GraphSONModuleV2d0(final boolean normalize) {
            super("graphson-2.0");

            /////////////////////// SERIALIZERS ////////////////////////////

            // graph
            addSerializer(Edge.class, new GraphSONSerializersV2d0.EdgeJacksonSerializer(normalize));
            addSerializer(Vertex.class, new GraphSONSerializersV2d0.VertexJacksonSerializer(normalize));
            addSerializer(VertexProperty.class, new GraphSONSerializersV2d0.VertexPropertyJacksonSerializer(normalize, true));
            addSerializer(Property.class, new GraphSONSerializersV2d0.PropertyJacksonSerializer());
            addSerializer(Metrics.class, new GraphSONSerializersV2d0.MetricsJacksonSerializer());
            addSerializer(TraversalMetrics.class, new GraphSONSerializersV2d0.TraversalMetricsJacksonSerializer());
            addSerializer(TraversalExplanation.class, new GraphSONSerializersV2d0.TraversalExplanationJacksonSerializer());
            addSerializer(Path.class, new GraphSONSerializersV2d0.PathJacksonSerializer());
            addSerializer(DirectionalStarGraph.class, new StarGraphGraphSONSerializerV2d0(normalize));
            addSerializer(Tree.class, new GraphSONSerializersV2d0.TreeJacksonSerializer());

            // java.util
            addSerializer(Map.Entry.class, new JavaUtilSerializersV2d0.MapEntryJacksonSerializer());

            // need to explicitly add serializers for those types because Jackson doesn't do it at all.
            addSerializer(Integer.class, new GraphSONSerializersV2d0.IntegerGraphSONSerializer());
            addSerializer(Double.class, new GraphSONSerializersV2d0.DoubleGraphSONSerializer());

            // traversal
            addSerializer(Traversal.class, new GraphSONTraversalSerializersV2d0.TraversalJacksonSerializer());
            addSerializer(Bytecode.class, new GraphSONTraversalSerializersV2d0.BytecodeJacksonSerializer());
            Stream.of(VertexProperty.Cardinality.class,
                    Column.class,
                    Direction.class,
                    Operator.class,
                    Order.class,
                    Pop.class,
                    SackFunctions.Barrier.class,
                    Scope.class,
                    TraversalOptionParent.Pick.class,
                    T.class).forEach(e -> addSerializer(e, new GraphSONTraversalSerializersV2d0.EnumJacksonSerializer()));
            addSerializer(P.class, new GraphSONTraversalSerializersV2d0.PJacksonSerializer());
            addSerializer(Lambda.class, new GraphSONTraversalSerializersV2d0.LambdaJacksonSerializer());
            addSerializer(Bytecode.Binding.class, new GraphSONTraversalSerializersV2d0.BindingJacksonSerializer());
            addSerializer(Traverser.class, new GraphSONTraversalSerializersV2d0.TraverserJacksonSerializer());
            addSerializer(TraversalStrategy.class, new GraphSONTraversalSerializersV2d0.TraversalStrategyJacksonSerializer());

            /////////////////////// DESERIALIZERS ////////////////////////////

            // Tinkerpop Graph
            addDeserializer(Vertex.class, new GraphSONSerializersV2d0.VertexJacksonDeserializer());
            addDeserializer(Edge.class, new GraphSONSerializersV2d0.EdgeJacksonDeserializer());
            addDeserializer(Property.class, new GraphSONSerializersV2d0.PropertyJacksonDeserializer());
            addDeserializer(Path.class, new GraphSONSerializersV2d0.PathJacksonDeserializer());
            addDeserializer(VertexProperty.class, new GraphSONSerializersV2d0.VertexPropertyJacksonDeserializer());
            addDeserializer(Metrics.class, new GraphSONSerializersV2d0.MetricsJacksonDeserializer());
            addDeserializer(TraversalMetrics.class, new GraphSONSerializersV2d0.TraversalMetricsJacksonDeserializer());
            addDeserializer(Tree.class, new GraphSONSerializersV2d0.TreeJacksonDeserializer());

            // numbers
            addDeserializer(Integer.class, new GraphSONSerializersV2d0.IntegerJackonsDeserializer());
            addDeserializer(Double.class, new GraphSONSerializersV2d0.DoubleJackonsDeserializer());

            // traversal
            addDeserializer(Bytecode.class, new GraphSONTraversalSerializersV2d0.BytecodeJacksonDeserializer());
            addDeserializer(Bytecode.Binding.class, new GraphSONTraversalSerializersV2d0.BindingJacksonDeserializer());
            Stream.of(VertexProperty.Cardinality.values(),
                    Column.values(),
                    Direction.values(),
                    Operator.values(),
                    Order.values(),
                    Pop.values(),
                    SackFunctions.Barrier.values(),
                    Scope.values(),
                    TraversalOptionParent.Pick.values(),
                    T.values()).flatMap(Stream::of).forEach(e -> addDeserializer(e.getClass(), new GraphSONTraversalSerializersV2d0.EnumJacksonDeserializer(e.getDeclaringClass())));
            addDeserializer(P.class, new GraphSONTraversalSerializersV2d0.PJacksonDeserializer());
            addDeserializer(Lambda.class, new GraphSONTraversalSerializersV2d0.LambdaJacksonDeserializer());
            addDeserializer(Traverser.class, new GraphSONTraversalSerializersV2d0.TraverserJacksonDeserializer());
            Arrays.asList(
                    ConnectiveStrategy.class,
                    ElementIdStrategy.class,
                    EventStrategy.class,
                    HaltedTraverserStrategy.class,
                    PartitionStrategy.class,
                    SubgraphStrategy.class,
                    LazyBarrierStrategy.class,
                    MatchAlgorithmStrategy.class,
                    AdjacentToIncidentStrategy.class,
                    FilterRankingStrategy.class,
                    IdentityRemovalStrategy.class,
                    IncidentToAdjacentStrategy.class,
                    InlineFilterStrategy.class,
                    MatchPredicateStrategy.class,
                    OrderLimitStrategy.class,
                    PathProcessorStrategy.class,
                    PathRetractionStrategy.class,
                    RangeByIsCountStrategy.class,
                    RepeatUnrollStrategy.class,
                    ComputerVerificationStrategy.class,
                    LambdaRestrictionStrategy.class,
                    ReadOnlyStrategy.class,
                    StandardVerificationStrategy.class,
                    //
                    GraphFilterStrategy.class,
                    VertexProgramStrategy.class
            ).forEach(strategy -> addDeserializer(strategy, new GraphSONTraversalSerializersV2d0.TraversalStrategyProxyJacksonDeserializer(strategy)));
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
            public GraphSONModule create(final boolean normalize) {
                return new GraphSONModuleV2d0(normalize);
            }

        }
    }

    /**
     * Version 1.0 of GraphSON.
     */
    static final class GraphSONModuleV1d0 extends GraphSONModule {

        /**
         * Constructs a new object.
         */
        protected GraphSONModuleV1d0(final boolean normalize) {
            super("graphson-1.0");
            // graph
            addSerializer(Edge.class, new GraphSONSerializersV1d0.EdgeJacksonSerializer(normalize));
            addSerializer(Vertex.class, new GraphSONSerializersV1d0.VertexJacksonSerializer(normalize));
            addSerializer(VertexProperty.class, new GraphSONSerializersV1d0.VertexPropertyJacksonSerializer(normalize));
            addSerializer(Property.class, new GraphSONSerializersV1d0.PropertyJacksonSerializer());
            addSerializer(TraversalMetrics.class, new GraphSONSerializersV1d0.TraversalMetricsJacksonSerializer());
            addSerializer(TraversalExplanation.class, new GraphSONSerializersV1d0.TraversalExplanationJacksonSerializer());
            addSerializer(Path.class, new GraphSONSerializersV1d0.PathJacksonSerializer());
            addSerializer(DirectionalStarGraph.class, new StarGraphGraphSONSerializerV1d0(normalize));
            addSerializer(Tree.class, new GraphSONSerializersV1d0.TreeJacksonSerializer());

            // java.util
            addSerializer(Map.Entry.class, new JavaUtilSerializersV1d0.MapEntryJacksonSerializer());

            // java.time
            addSerializer(Duration.class, new JavaTimeSerializersV1d0.DurationJacksonSerializer());
            addSerializer(Instant.class, new JavaTimeSerializersV1d0.InstantJacksonSerializer());
            addSerializer(LocalDate.class, new JavaTimeSerializersV1d0.LocalDateJacksonSerializer());
            addSerializer(LocalDateTime.class, new JavaTimeSerializersV1d0.LocalDateTimeJacksonSerializer());
            addSerializer(LocalTime.class, new JavaTimeSerializersV1d0.LocalTimeJacksonSerializer());
            addSerializer(MonthDay.class, new JavaTimeSerializersV1d0.MonthDayJacksonSerializer());
            addSerializer(OffsetDateTime.class, new JavaTimeSerializersV1d0.OffsetDateTimeJacksonSerializer());
            addSerializer(OffsetTime.class, new JavaTimeSerializersV1d0.OffsetTimeJacksonSerializer());
            addSerializer(Period.class, new JavaTimeSerializersV1d0.PeriodJacksonSerializer());
            addSerializer(Year.class, new JavaTimeSerializersV1d0.YearJacksonSerializer());
            addSerializer(YearMonth.class, new JavaTimeSerializersV1d0.YearMonthJacksonSerializer());
            addSerializer(ZonedDateTime.class, new JavaTimeSerializersV1d0.ZonedDateTimeJacksonSerializer());
            addSerializer(ZoneOffset.class, new JavaTimeSerializersV1d0.ZoneOffsetJacksonSerializer());

            addDeserializer(Duration.class, new JavaTimeSerializersV1d0.DurationJacksonDeserializer());
            addDeserializer(Instant.class, new JavaTimeSerializersV1d0.InstantJacksonDeserializer());
            addDeserializer(LocalDate.class, new JavaTimeSerializersV1d0.LocalDateJacksonDeserializer());
            addDeserializer(LocalDateTime.class, new JavaTimeSerializersV1d0.LocalDateTimeJacksonDeserializer());
            addDeserializer(LocalTime.class, new JavaTimeSerializersV1d0.LocalTimeJacksonDeserializer());
            addDeserializer(MonthDay.class, new JavaTimeSerializersV1d0.MonthDayJacksonDeserializer());
            addDeserializer(OffsetDateTime.class, new JavaTimeSerializersV1d0.OffsetDateTimeJacksonDeserializer());
            addDeserializer(OffsetTime.class, new JavaTimeSerializersV1d0.OffsetTimeJacksonDeserializer());
            addDeserializer(Period.class, new JavaTimeSerializersV1d0.PeriodJacksonDeserializer());
            addDeserializer(Year.class, new JavaTimeSerializersV1d0.YearJacksonDeserializer());
            addDeserializer(YearMonth.class, new JavaTimeSerializersV1d0.YearMonthJacksonDeserializer());
            addDeserializer(ZonedDateTime.class, new JavaTimeSerializersV1d0.ZonedDateTimeJacksonDeserializer());
            addDeserializer(ZoneOffset.class, new JavaTimeSerializersV1d0.ZoneOffsetJacksonDeserializer());
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
            public GraphSONModule create(final boolean normalize) {
                return new GraphSONModuleV1d0(normalize);
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
         * @param normalize when set to true, keys and objects are ordered to ensure that they are the occur in
         *                  the same order.
         */
        GraphSONModule create(final boolean normalize);
    }
}
