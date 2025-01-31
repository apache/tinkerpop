/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.CombinedConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.tinkerpop.gremlin.language.translator.GremlinTranslator;
import org.apache.tinkerpop.gremlin.language.translator.Translator;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clone.CloneVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.ClusterCountMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.ClusterPopulationMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.MemoryTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponent;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressure;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.finalization.ComputerFinalizationStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.MessagePassingReductionStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification.VertexProgramRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ReferenceElementStrategy;
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
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.LegacyGraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoClassResolverV1;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoClassResolverV3;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class CoreImports {

    private final static Set<Class> CLASS_IMPORTS = new LinkedHashSet<>();
    private final static Set<Field> FIELD_IMPORTS = new LinkedHashSet<>();
    private final static Set<Method> METHOD_IMPORTS = new LinkedHashSet<>();
    private final static Set<Enum> ENUM_IMPORTS = new LinkedHashSet<>();

    static {
        /////////////
        // CLASSES //
        /////////////

        // structure
        CLASS_IMPORTS.add(Edge.class);
        CLASS_IMPORTS.add(Element.class);
        CLASS_IMPORTS.add(Graph.class);
        CLASS_IMPORTS.add(Property.class);
        CLASS_IMPORTS.add(Transaction.class);
        CLASS_IMPORTS.add(Vertex.class);
        CLASS_IMPORTS.add(VertexProperty.class);
        CLASS_IMPORTS.add(GraphFactory.class);
        CLASS_IMPORTS.add(ElementHelper.class);
        CLASS_IMPORTS.add(ReferenceEdge.class);
        CLASS_IMPORTS.add(ReferenceProperty.class);
        CLASS_IMPORTS.add(ReferenceVertex.class);
        CLASS_IMPORTS.add(ReferenceVertexProperty.class);
        
        // tokens
        CLASS_IMPORTS.add(SackFunctions.class);
        CLASS_IMPORTS.add(SackFunctions.Barrier.class);
        CLASS_IMPORTS.add(VertexProperty.Cardinality.class);
        CLASS_IMPORTS.add(Column.class);
        CLASS_IMPORTS.add(Direction.class);
        CLASS_IMPORTS.add(DT.class);
        CLASS_IMPORTS.add(Merge.class);
        CLASS_IMPORTS.add(Operator.class);
        CLASS_IMPORTS.add(Order.class);
        CLASS_IMPORTS.add(Pop.class);
        CLASS_IMPORTS.add(Scope.class);
        CLASS_IMPORTS.add(T.class);
        CLASS_IMPORTS.add(TraversalOptionParent.class);
        CLASS_IMPORTS.add(Pick.class);
        CLASS_IMPORTS.add(P.class);
        CLASS_IMPORTS.add(TextP.class);
        CLASS_IMPORTS.add(WithOptions.class);
        // remote
        CLASS_IMPORTS.add(RemoteConnection.class);
        CLASS_IMPORTS.add(EmptyGraph.class);
        // io
        CLASS_IMPORTS.add(GraphReader.class);
        CLASS_IMPORTS.add(GraphWriter.class);
        CLASS_IMPORTS.add(Io.class);
        CLASS_IMPORTS.add(IO.class);
        CLASS_IMPORTS.add(IoCore.class);
        CLASS_IMPORTS.add(Storage.class);
        CLASS_IMPORTS.add(GraphMLIo.class);
        CLASS_IMPORTS.add(GraphMLMapper.class);
        CLASS_IMPORTS.add(GraphMLReader.class);
        CLASS_IMPORTS.add(GraphMLWriter.class);
        CLASS_IMPORTS.add(GraphSONIo.class);
        CLASS_IMPORTS.add(GraphSONMapper.class);
        CLASS_IMPORTS.add(GraphSONReader.class);
        CLASS_IMPORTS.add(GraphSONTokens.class);
        CLASS_IMPORTS.add(GraphSONVersion.class);
        CLASS_IMPORTS.add(GraphSONWriter.class);
        CLASS_IMPORTS.add(LegacyGraphSONReader.class);
        CLASS_IMPORTS.add(GryoClassResolverV1.class);
        CLASS_IMPORTS.add(GryoClassResolverV3.class);
        CLASS_IMPORTS.add(GryoIo.class);
        CLASS_IMPORTS.add(GryoMapper.class);
        CLASS_IMPORTS.add(GryoReader.class);
        CLASS_IMPORTS.add(GryoVersion.class);
        CLASS_IMPORTS.add(GryoWriter.class);
        // configuration
        CLASS_IMPORTS.add(BaseConfiguration.class);
        CLASS_IMPORTS.add(CombinedConfiguration.class);
        CLASS_IMPORTS.add(CompositeConfiguration.class);
        CLASS_IMPORTS.add(Configuration.class);
        CLASS_IMPORTS.add(ConfigurationUtils.class);
        CLASS_IMPORTS.add(HierarchicalConfiguration.class);
        CLASS_IMPORTS.add(MapConfiguration.class);
        CLASS_IMPORTS.add(PropertiesConfiguration.class);
        CLASS_IMPORTS.add(SubsetConfiguration.class);
        CLASS_IMPORTS.add(XMLConfiguration.class);
        CLASS_IMPORTS.add(Configurations.class);
        // strategies
        CLASS_IMPORTS.add(ConnectiveStrategy.class);
        CLASS_IMPORTS.add(ComputerFinalizationStrategy.class);
        CLASS_IMPORTS.add(ElementIdStrategy.class);
        CLASS_IMPORTS.add(EventStrategy.class);
        CLASS_IMPORTS.add(HaltedTraverserStrategy.class);
        CLASS_IMPORTS.add(InlineFilterStrategy.class);
        CLASS_IMPORTS.add(MessagePassingReductionStrategy.class);
        CLASS_IMPORTS.add(OptionsStrategy.class);
        CLASS_IMPORTS.add(PartitionStrategy.class);
        CLASS_IMPORTS.add(ReservedKeysVerificationStrategy.class);
        CLASS_IMPORTS.add(SubgraphStrategy.class);
        CLASS_IMPORTS.add(LazyBarrierStrategy.class);
        CLASS_IMPORTS.add(MatchAlgorithmStrategy.class);
        CLASS_IMPORTS.add(ProfileStrategy.class);
        CLASS_IMPORTS.add(AdjacentToIncidentStrategy.class);
        CLASS_IMPORTS.add(ByModulatorOptimizationStrategy.class);
        CLASS_IMPORTS.add(ProductiveByStrategy.class);
        CLASS_IMPORTS.add(CountStrategy.class);
        CLASS_IMPORTS.add(FilterRankingStrategy.class);
        CLASS_IMPORTS.add(IdentityRemovalStrategy.class);
        CLASS_IMPORTS.add(IncidentToAdjacentStrategy.class);
        CLASS_IMPORTS.add(MatchPredicateStrategy.class);
        CLASS_IMPORTS.add(EarlyLimitStrategy.class);
        CLASS_IMPORTS.add(OrderLimitStrategy.class);
        CLASS_IMPORTS.add(PathProcessorStrategy.class);
        CLASS_IMPORTS.add(ComputerVerificationStrategy.class);
        CLASS_IMPORTS.add(LambdaRestrictionStrategy.class);
        CLASS_IMPORTS.add(PathRetractionStrategy.class);
        CLASS_IMPORTS.add(ReadOnlyStrategy.class);
        CLASS_IMPORTS.add(ReferenceElementStrategy.class);
        CLASS_IMPORTS.add(RepeatUnrollStrategy.class);
        CLASS_IMPORTS.add(SeedStrategy.class);
        CLASS_IMPORTS.add(StandardVerificationStrategy.class);
        CLASS_IMPORTS.add(EdgeLabelVerificationStrategy.class);
        CLASS_IMPORTS.add(VertexProgramRestrictionStrategy.class);
        // graph traversal
        CLASS_IMPORTS.add(AnonymousTraversalSource.class);
        CLASS_IMPORTS.add(__.class);
        CLASS_IMPORTS.add(GraphTraversal.class);
        CLASS_IMPORTS.add(GraphTraversalSource.class);
        CLASS_IMPORTS.add(Traversal.class);
        CLASS_IMPORTS.add(TraversalMetrics.class);
        // graph computer
        CLASS_IMPORTS.add(Computer.class);
        CLASS_IMPORTS.add(ComputerResult.class);
        CLASS_IMPORTS.add(ConnectedComponent.class);
        CLASS_IMPORTS.add(ConnectedComponentVertexProgram.class);
        CLASS_IMPORTS.add(GraphComputer.class);
        CLASS_IMPORTS.add(Memory.class);
        CLASS_IMPORTS.add(VertexProgram.class);
        CLASS_IMPORTS.add(CloneVertexProgram.class);
        CLASS_IMPORTS.add(ClusterCountMapReduce.class);
        CLASS_IMPORTS.add(ClusterPopulationMapReduce.class);
        CLASS_IMPORTS.add(MemoryTraversalSideEffects.class);
        CLASS_IMPORTS.add(PeerPressure.class);
        CLASS_IMPORTS.add(PeerPressureVertexProgram.class);
        CLASS_IMPORTS.add(PageRank.class);
        CLASS_IMPORTS.add(PageRankMapReduce.class);
        CLASS_IMPORTS.add(PageRankVertexProgram.class);
        CLASS_IMPORTS.add(ShortestPath.class);
        CLASS_IMPORTS.add(ShortestPathVertexProgram.class);
        CLASS_IMPORTS.add(GraphFilterStrategy.class);
        CLASS_IMPORTS.add(TraversalVertexProgram.class);
        CLASS_IMPORTS.add(VertexProgramStrategy.class);
        // utils
        CLASS_IMPORTS.add(Gremlin.class);
        CLASS_IMPORTS.add(IteratorUtils.class);
        CLASS_IMPORTS.add(TimeUtil.class);
        CLASS_IMPORTS.add(Lambda.class);
        CLASS_IMPORTS.add(java.util.Date.class);
        CLASS_IMPORTS.add(java.sql.Timestamp.class);
        CLASS_IMPORTS.add(java.util.UUID.class);
        CLASS_IMPORTS.add(GremlinTranslator.class);
        CLASS_IMPORTS.add(Translator.class);

        /////////////
        // METHODS //
        /////////////

        uniqueMethods(IoCore.class).forEach(METHOD_IMPORTS::add);
        uniqueMethods(P.class).forEach(METHOD_IMPORTS::add);
        uniqueMethods(AnonymousTraversalSource.class).forEach(METHOD_IMPORTS::add);
        uniqueMethods(TextP.class).forEach(METHOD_IMPORTS::add);
        uniqueMethods(__.class).filter(m -> !m.getName().equals("__")).forEach(METHOD_IMPORTS::add);
        uniqueMethods(Computer.class).forEach(METHOD_IMPORTS::add);
        uniqueMethods(TimeUtil.class).forEach(METHOD_IMPORTS::add);
        uniqueMethods(Lambda.class).forEach(METHOD_IMPORTS::add);
        try {
            METHOD_IMPORTS.add(DatetimeHelper.class.getMethod("datetime", String.class));
            METHOD_IMPORTS.add(DatetimeHelper.class.getMethod("datetime"));
        } catch (Exception ex) {
            throw new IllegalStateException("Could not load datetime() function to imports");
        }

        ///////////
        // ENUMS //
        ///////////

        Collections.addAll(ENUM_IMPORTS, SackFunctions.Barrier.values());
        Collections.addAll(ENUM_IMPORTS, VertexProperty.Cardinality.values());
        Collections.addAll(ENUM_IMPORTS, Column.values());
        Collections.addAll(ENUM_IMPORTS, Direction.values());
        Collections.addAll(ENUM_IMPORTS, DT.values());
        Collections.addAll(ENUM_IMPORTS, Merge.values());
        Collections.addAll(ENUM_IMPORTS, Operator.values());
        Collections.addAll(ENUM_IMPORTS, Order.values());
        Collections.addAll(ENUM_IMPORTS, Pop.values());
        Collections.addAll(ENUM_IMPORTS, Scope.values());
        Collections.addAll(ENUM_IMPORTS, T.values());
        Collections.addAll(ENUM_IMPORTS, Pick.values());

        ////////////
        // FIELDS //
        ////////////

        try {
            FIELD_IMPORTS.add(Double.class.getField("NaN"));
            FIELD_IMPORTS.add(Direction.class.getField("from"));
            FIELD_IMPORTS.add(Direction.class.getField("to"));
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private CoreImports() {
        // static methods only, do not instantiate class
    }

    public static Set<Class> getClassImports() {
        return Collections.unmodifiableSet(CLASS_IMPORTS);
    }

    public static Set<Method> getMethodImports() {
        return Collections.unmodifiableSet(METHOD_IMPORTS);
    }

    public static Set<Enum> getEnumImports() {
        return Collections.unmodifiableSet(ENUM_IMPORTS);
    }

    public static Set<Field> getFieldImports() {
        return Collections.unmodifiableSet(FIELD_IMPORTS);
    }

    /**
     * Filters to unique method names on each class.
     */
    private static Stream<Method> uniqueMethods(final Class<?> clazz) {
        final Set<String> unique = new LinkedHashSet<>();
        return Stream.of(clazz.getMethods())
                .filter(m -> Modifier.isStatic(m.getModifiers()))
                .map(m -> Pair.with(generateMethodDescriptor(m), m))
                .filter(p -> {
                    final boolean exists = unique.contains(p.getValue0());
                    if (!exists) unique.add(p.getValue0());
                    return !exists;
                })
                .map(Pair::getValue1);
    }

    private static String generateMethodDescriptor(final Method m) {
        return m.getDeclaringClass().getCanonicalName() + "." + m.getName();
    }
}
