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
package org.apache.tinkerpop.gremlin.process.traversal.dsl.graph;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStartStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DeclarativeMatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeEdgeStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A {@code GraphTraversalSource} is the primary DSL of the Gremlin traversal machine.
 * It provides access to all the configurations and steps for Turing complete graph computing.
 * Any DSL can be constructed based on the methods of both {@code GraphTraversalSource} and {@link GraphTraversal}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphTraversalSource implements TraversalSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphTraversalSource.class);

    protected transient RemoteConnection connection;
    protected final Graph graph;
    protected TraversalStrategies strategies;
    protected GremlinLang gremlinLang = new GremlinLang();

    ////////////////

    public static final class Symbols {

        private Symbols() {
            // static fields only
        }

        public static final String withBulk = "withBulk";
        public static final String withPath = "withPath";
        public static final String tx = "tx";

    }

    ////////////////

    public GraphTraversalSource(final Graph graph, final TraversalStrategies traversalStrategies) {
        this.graph = graph;
        this.strategies = traversalStrategies;
    }

    public GraphTraversalSource(final Graph graph) {
        this(graph, TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
    }

    public GraphTraversalSource(final RemoteConnection connection) {
        this(EmptyGraph.instance(), TraversalStrategies.GlobalCache.getStrategies(EmptyGraph.class).clone());
        this.connection = connection;
        this.strategies.addStrategies(new RemoteStrategy(connection));
        if (connection.getPdtRegistry() != null) {
            this.gremlinLang.setPdtRegistry(connection.getPdtRegistry());
        }
    }

    @Override
    public Optional<Class<?>> getAnonymousTraversalClass() {
        return Optional.of(__.class);
    }

    @Override
    public TraversalStrategies getStrategies() {
        return this.strategies;
    }

    @Override
    public Graph getGraph() {
        return this.graph;
    }

    @Override
    public GremlinLang getGremlinLang() {
        return this.gremlinLang;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public GraphTraversalSource clone() {
        try {
            final GraphTraversalSource clone = (GraphTraversalSource) super.clone();
            clone.strategies = this.strategies.clone();
            clone.gremlinLang = this.gremlinLang.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    //// CONFIGURATIONS

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource with(final String key) {
        return (GraphTraversalSource) TraversalSource.super.with(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource with(final String key, final Object value) {
        return (GraphTraversalSource) TraversalSource.super.with(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource withStrategies(final TraversalStrategy... traversalStrategies) {
        return (GraphTraversalSource) TraversalSource.super.withStrategies(traversalStrategies);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings({"unchecked"})
    public GraphTraversalSource withoutStrategies(final Class<? extends TraversalStrategy>... traversalStrategyClasses) {
        return (GraphTraversalSource) TraversalSource.super.withoutStrategies(traversalStrategyClasses);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource withComputer(final Computer computer) {
        return (GraphTraversalSource) TraversalSource.super.withComputer(computer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource withComputer(final Class<? extends GraphComputer> graphComputerClass) {
        return (GraphTraversalSource) TraversalSource.super.withComputer(graphComputerClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource withComputer() {
        return (GraphTraversalSource) TraversalSource.super.withComputer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue, final BinaryOperator<A> reducer) {
        return (GraphTraversalSource) TraversalSource.super.withSideEffect(key, initialValue, reducer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final A initialValue, final BinaryOperator<A> reducer) {
        return (GraphTraversalSource) TraversalSource.super.withSideEffect(key, initialValue, reducer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final A initialValue) {
        return (GraphTraversalSource) TraversalSource.super.withSideEffect(key, initialValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue) {
        return (GraphTraversalSource) TraversalSource.super.withSideEffect(key, initialValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, splitOperator, mergeOperator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, splitOperator, mergeOperator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSack(final A initialValue) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, splitOperator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, splitOperator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, mergeOperator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, mergeOperator);
    }

    public GraphTraversalSource withBulk(final boolean useBulk) {
        if (useBulk)
            return this;
        final GraphTraversalSource clone = this.clone();
        RequirementsStrategy.addRequirements(clone.getStrategies(), TraverserRequirement.ONE_BULK);
        clone.gremlinLang.addSource(Symbols.withBulk, useBulk);
        return clone;
    }

    public GraphTraversalSource withPath() {
        final GraphTraversalSource clone = this.clone();
        RequirementsStrategy.addRequirements(clone.getStrategies(), TraverserRequirement.PATH);
        clone.gremlinLang.addSource(Symbols.withPath);
        return clone;
    }

    //// SPAWNS

    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the specified label. If the {@code label} is
     * {@code null} then it will default to {@link Vertex#DEFAULT_LABEL}.
     *
     * @since 3.1.0-incubating
     */
    public GraphTraversal<Vertex, Vertex> addV(final String vertexLabel) {
        if (null == vertexLabel) throw new IllegalArgumentException("vertexLabel cannot be null");
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.addV, vertexLabel);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddVertexStartStepPlaceholder(traversal, vertexLabel));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the label as determined by a {@link Traversal}. If the
     * {@code vertexLabelTraversal} is {@code null} then it will default to {@link Vertex#DEFAULT_LABEL}.
     *
     * @since 3.3.1
     */
    public GraphTraversal<Vertex, Vertex> addV(final Traversal<?, String> vertexLabelTraversal) {
        if (null == vertexLabelTraversal) throw new IllegalArgumentException("vertexLabelTraversal cannot be null");
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.addV, vertexLabelTraversal);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddVertexStartStepPlaceholder(traversal, vertexLabelTraversal.asAdmin()));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the default label.
     *
     * @since 3.1.0-incubating
     */
    public GraphTraversal<Vertex, Vertex> addV() {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.addV);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddVertexStartStepPlaceholder(traversal, (String) null));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the specified label. If the {@code label} is
     * {@code null} then it will default to {@link Vertex#DEFAULT_LABEL}.
     *
     * @since 3.8.0
     */
    public GraphTraversal<Vertex, Vertex> addV(final GValue<String> vertexLabel) {
        if (null == vertexLabel) throw new IllegalArgumentException("vertexLabel cannot be null");
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.addV, vertexLabel);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddVertexStartStepPlaceholder(traversal, vertexLabel));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with multiple labels.
     * Creates the vertex with the first label, then adds the remaining labels.
     *
     * @param label1     the first label
     * @param label2     the second label
     * @param moreLabels additional labels
     * @return the traversal with the vertex added
     * @since 4.0.0
     */
    public GraphTraversal<Vertex, Vertex> addV(final String label1, final String label2, final String... moreLabels) {
        if (null == label1) throw new IllegalArgumentException("vertexLabel cannot be null");
        if (null == label2) throw new IllegalArgumentException("vertexLabel cannot be null");
        for (final String l : moreLabels) {
            if (null == l) throw new IllegalArgumentException("vertexLabel cannot be null");
        }
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.addV, label1, label2, moreLabels);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        final Set<String> allLabels = new LinkedHashSet<>();
        allLabels.add(label1);
        allLabels.add(label2);
        Collections.addAll(allLabels, moreLabels);
        return traversal.addStep(new AddVertexStartStepPlaceholder(traversal, allLabels));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding an edge with the specified label.
     *
     * @since 3.1.0-incubating
     */
    public GraphTraversal<Edge, Edge> addE(final String label) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.addE, label);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddEdgeStartStepPlaceholder(traversal, label));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a edge with a label as specified by the provided {@link Traversal}.
     *
     * @since 3.3.1
     */
    public GraphTraversal<Edge, Edge> addE(final Traversal<?, String> edgeLabelTraversal) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.addE, edgeLabelTraversal);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddEdgeStartStepPlaceholder(traversal, edgeLabelTraversal.asAdmin()));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding an edge with the specified label.
     *
     * @since 3.8.0
     */
    public GraphTraversal<Edge, Edge> addE(final GValue<String> label) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.addE, label);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddEdgeStartStepPlaceholder(traversal, label));
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Vertex} using a
     * {@code Map} as an argument. The {@code Map} represents search criteria and will match each of the supplied
     * key/value pairs where the keys may be {@code String} property values or a value of {@link T}. If a match is not
     * made it will use that search criteria to create the new {@link Vertex}.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} or a {@code String}.
     * @since 3.6.0
     */
    public GraphTraversal<Vertex, Vertex> mergeV(final Map<Object, Object> searchCreate) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.mergeV, searchCreate);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new MergeVertexStepPlaceholder<>(traversal, true, searchCreate));
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Vertex} using a
     * {@code Map} as an argument. The {@code Map} represents search criteria and will match each of the supplied
     * key/value pairs where the keys may be {@code String} property values or a value of {@link T}. If a match is not
     * made it will use that search criteria to create the new {@link Vertex}.
     *
     * @param searchCreate This anonymous {@link Traversal} must produce a {@code Map} that may have a keys of
     * {@link T} or a {@code String}.
     * @since 3.6.0
     */
    public <S> GraphTraversal<S, Vertex> mergeV(final Traversal<?, Map<Object, Object>> searchCreate) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.mergeV, searchCreate);
        final GraphTraversal.Admin<S, Vertex> traversal = new DefaultGraphTraversal<>(clone);

        final MergeVertexStepPlaceholder<S> step = null == searchCreate ? new MergeVertexStepPlaceholder(traversal, true, (Map) null) :
                new MergeVertexStepPlaceholder(traversal, true, searchCreate.asAdmin());

        return traversal.addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Vertex} using a
     * {@code Map} as an argument. The {@code Map} represents search criteria and will match each of the supplied
     * key/value pairs where the keys may be {@code String} property values or a value of {@link T}. If a match is not
     * made it will use that search criteria to create the new {@link Vertex}.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} or a {@code String}.
     * @since 3.8.0
     */
    public GraphTraversal<Vertex, Vertex> mergeV(final GValue<Map<?, ?>> searchCreate) {
        final GraphTraversalSource clone = GraphTraversalSource.this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.mergeV, searchCreate);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        final MergeVertexStepPlaceholder<Vertex> step = null == searchCreate ? new MergeVertexStepPlaceholder(traversal, true, (Map) null) :
                new MergeVertexStepPlaceholder(traversal, true, searchCreate);

        return traversal.addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Edge} using a
     * {@code Map} as an argument.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} {@link Direction} or a {@code String}.
     * @since 3.6.0
     */
    public GraphTraversal<Edge, Edge> mergeE(final Map<Object, Object> searchCreate) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.mergeE, searchCreate);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new MergeEdgeStepPlaceholder<>(traversal, true, searchCreate));
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Edge} using a
     * {@code Map} as an argument.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} {@link Direction} or a {@code String}.
     * @since 3.6.0
     */
    public GraphTraversal<Edge, Edge> mergeE(final Traversal<?, Map<Object, Object>> searchCreate) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.mergeE, searchCreate);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);

        final MergeEdgeStepPlaceholder step = null == searchCreate ? new MergeEdgeStepPlaceholder(traversal, true,  (Map) null) :
                new MergeEdgeStepPlaceholder(traversal, true, searchCreate.asAdmin());

        return traversal.addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Edge} using a
     * {@code Map} as an argument.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} {@link Direction} or a {@code String}.
     * @since 3.8.0
     */
    public GraphTraversal<Edge, Edge> mergeE(final GValue<Map<Object, Object>> searchCreate) {
        final GraphTraversalSource clone = GraphTraversalSource.this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.mergeE, searchCreate);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        final MergeEdgeStepPlaceholder<Edge> step = null == searchCreate ? new MergeEdgeStepPlaceholder(traversal, true,  (Map) null) :
                new MergeEdgeStepPlaceholder(traversal, true, searchCreate);

        return traversal.addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} starting it with arbitrary values.
     */
    public <S> GraphTraversal<S, S> inject(S... starts) {
        // a single null is [null]
        final S[] s = null == starts ? (S[]) new Object[] { null } : starts;
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.inject, s);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new InjectStep<S>(traversal, s));
    }

    /**
     * Spawns a {@link GraphTraversal} starting with all vertices or some subset of vertices as specified by their
     * unique identifier.
     *
     * @since 3.0.0-incubating
     */
    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        // a single null is [null]
        final Object[] ids = null == vertexIds ? new Object[] { null } : vertexIds;
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.V, ids);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        GraphStepContract<Vertex, Vertex> step;
        if (GValue.containsGValues(ids)) {
            step = new GraphStepPlaceholder<>(traversal, Vertex.class, true, GValue.ensureGValues(ids));
        } else {
            step = new GraphStep<>(traversal, Vertex.class, true, ids);
        }
        return traversal.addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} starting with all edges or some subset of edges as specified by their unique
     * identifier.
     *
     * @since 3.0.0-incubating
     */
    public GraphTraversal<Edge, Edge> E(final Object... edgeIds) {
        // a single null is [null]
        final Object[] ids = null == edgeIds ? new Object[] { null } : edgeIds;
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.E, ids);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        GraphStepContract<Edge, Edge> step;
        if (GValue.containsGValues(ids)) {
            step = new GraphStepPlaceholder<>(traversal, Edge.class, true, GValue.ensureGValues(ids));
        } else {
            step = new GraphStep<>(traversal, Edge.class, true, ids);
        }
        return traversal.addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} starting with a list of available services.
     *
     * @since 3.6.0
     */
    public <S> GraphTraversal<S, S> call() {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.call);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new CallStep<>(traversal, true));
    }

    /**
     * Spawns a {@link GraphTraversal} starting with values produced by the specified service call with no parameters.
     *
     * @param service the name of the service call
     * @since 3.6.0
     */
    public <S> GraphTraversal<S, S> call(final String service) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.call, service);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new CallStep<>(traversal, true, service));
    }

    /**
     * Spawns a {@link GraphTraversal} starting with values produced by the specified service call with the specified
     * static parameters.
     *
     * @param service the name of the service call
     * @param params static parameter map (no nested traversals)
     * @since 3.6.0
     */
    public <S> GraphTraversal<S, S> call(final String service, final Map params) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.call, service, params);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new CallStep<>(traversal, true, service, params));
    }

    /**
     * Spawns a {@link GraphTraversal} starting with values produced by the specified service call with dynamic
     * parameters produced by the specified child traversal.
     *
     * @param service the name of the service call
     * @param childTraversal a traversal that will produce a Map of parameters for the service call when invoked.
     * @since 3.6.0
     */
    public <S> GraphTraversal<S, S> call(final String service, final Traversal<S, Map> childTraversal) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.call, service, childTraversal);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        final CallStep<S,S> step = null == childTraversal ? new CallStep(traversal, true, service) :
                new CallStep(traversal, true, service, new LinkedHashMap(), childTraversal.asAdmin());
        return traversal.addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} starting with values produced by the specified service call with both static and
     * dynamic parameters produced by the specified child traversal. These parameters will be merged at execution time
     * per the provider implementation. Reference implementation merges dynamic into static (dynamic will overwrite
     * static).
     *
     * @param service the name of the service call
     * @param params static parameter map (no nested traversals)
     * @param childTraversal a traversal that will produce a Map of parameters for the service call when invoked.
     * @since 3.6.0
     */
    public <S> GraphTraversal<S, S> call(final String service, final Map params, final Traversal<S, Map> childTraversal) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.call, service, params, childTraversal);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        final CallStep<S,S> step = null == childTraversal ? new CallStep(traversal, true, service, params) :
                new CallStep(traversal, true, service, params, childTraversal.asAdmin());
        return traversal.addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} starting with values produced by the specified service call with the specified
     * static parameters.
     *
     * @param service the name of the service call
     * @param params static parameter map (no nested traversals)
     * @since 3.8.0
     */
    public <S> GraphTraversal<S, S> call(final String service, final GValue<Map<?,?>> params) {
        final GraphTraversalSource clone = GraphTraversalSource.this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.call, service, params);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new CallStepPlaceholder<>(traversal, true, service, params));
    }

    /**
     * Spawns a {@link GraphTraversal} starting with values produced by the specified service call with the specified
     * static parameters.
     *
     * @param service the name of the service call
     * @param params static parameter map (no nested traversals)
     * @since 3.8.0
     */
    public <S> GraphTraversal<S, S> call(final String service, final GValue<Map<?,?>> params, final Traversal<S, Map<?,?>> childTraversal) {
        final GraphTraversalSource clone = GraphTraversalSource.this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.call, service, params, childTraversal);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new CallStepPlaceholder<>(traversal, true, service, params, childTraversal.asAdmin()));
    }

    /**
     * Spawns a {@link GraphTraversal} by executing a declarative pattern match query. The query language is not
     * prescribed or implemented by the framework; it defaults to {@code null}, meaning the graph
     * provider will use its native query language. Use {@code .with("queryLanguage", value)} to specify a
     * language explicitly. Consult the graph system you are using to determine what query language you can give to
     * this step.
     *
     * @param matchQuery the declarative query string
     * @return the traversal with an appended {@link DeclarativeMatchStep}.
     * @since 4.0.0
     */
    public <S> GraphTraversal<S, Map<String, Object>> match(final String matchQuery) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.match, matchQuery);
        final GraphTraversal.Admin<S, Map<String, Object>> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new DeclarativeMatchStep<>(traversal, matchQuery, null, null, true));
    }

    /**
     * Spawns a {@link GraphTraversal} by executing a declarative pattern match query. The query language is not
     * prescribed or implemented by the framework; it defaults to {@code null}, meaning the graph
     * provider will use its native query language. Use {@code .with("queryLanguage", value)} to specify a
     * language explicitly. Consult the graph system you are using to determine what query language you can give to
     * this step.
     *
     * @param matchQuery the declarative query string
     * @param params the query parameters (may be {@code null})
     * @return the traversal with an appended {@link DeclarativeMatchStep}.
     * @since 4.0.0
     */
    public <S> GraphTraversal<S, Map<String, Object>> match(final String matchQuery, final Map<String, Object> params) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.match, matchQuery, params);
        final GraphTraversal.Admin<S, Map<String, Object>> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new DeclarativeMatchStep<>(traversal, matchQuery, params, null, true));
    }

    /**
     * Merges the results of an arbitrary number of traversals.
     *
     * @param unionTraversals the traversals to merge
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#union-step" target="_blank">Reference Documentation - Union Step</a>
     * @since 3.7.0
     */
    public <S> GraphTraversal<S, S> union(final Traversal<?, S>... unionTraversals) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.union, unionTraversals);
        final GraphTraversal.Admin traversal = new DefaultGraphTraversal(clone);
        final UnionStep<?, S> step = new UnionStep<>(traversal, true, Arrays.copyOf(unionTraversals, unionTraversals.length, Traversal.Admin[].class));
        return traversal.addStep(step);
    }

    /**
     * Performs a read or write based operation on the {@link Graph} backing this {@code GraphTraversalSource}. This
     * step can be accompanied by the {@link GraphTraversal#with(String, Object)} modulator for further configuration
     * and must be accompanied by a {@link GraphTraversal#read()} or {@link GraphTraversal#write()} modulator step
     * which will terminate the traversal.
     *
     * @param file the name of file for which the read or write will apply - note that the context of how this
     *             parameter is used is wholly dependent on the implementation
     * @return the traversal with the {@link IoStep} added
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#io-step" target="_blank">Reference Documentation - IO Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#read-step" target="_blank">Reference Documentation - Read Step</a>
     * @see <a href="http://tinkerpop.apache.org/docs/${project.version}/reference/#write-step" target="_blank">Reference Documentation - Write Step</a>
     * @since 3.4.0
     */
    public <S> GraphTraversal<S, S> io(final String file) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.io, file);
        final GraphTraversal.Admin<S,S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new IoStep<S>(traversal, file));
    }

    /**
     * Proxies calls through to the underlying {@link Graph#tx()} or to the {@link RemoteConnection#tx()}.
     * <p>
     * When a remote connection is present, this method delegates to the connection's
     * {@link RemoteConnection#tx()} method, which returns an appropriate transaction
     * implementation for the remote connection type (e.g., {@code HttpRemoteTransaction}
     * for HTTP-based connections).
     *
     * @return A {@link Transaction} for managing transactional operations
     */
    public Transaction tx() {
        if (null == this.connection)
            return this.graph.tx();
        else
            return this.connection.tx();
    }

    /**
     * Runs the supplied unit of work inside a single transaction, managing the transaction lifecycle automatically.
     * <p>
     * This is the no-return (action) form of {@link #evaluateInTx(Function)}. It is a thin convenience wrapper that
     * obtains a {@link Transaction} via {@link #tx()}, {@link Transaction#begin() begins} it, invokes {@code txWork}
     * with the transaction-bound {@link GraphTraversalSource} ({@code gtx}), and then {@link Transaction#commit()
     * commits} on normal completion. If {@code txWork} throws, the transaction is {@link Transaction#rollback() rolled
     * back} and the original error is re-thrown unchanged. Because the lifecycle is driven through {@link #tx()}, the
     * underlying transaction semantics (embedded thread-bound vs. remote server session) are whatever the underlying
     * {@code begin()}/{@code commit()}/{@code rollback()} provide.
     * <p>
     * This is a <strong>single-shot</strong> operation - exactly one attempt is made, with no automatic retry. The
     * lambda receives the transactional {@code gtx} and should issue its traversals against that source only.
     *
     * @param txWork the unit of work to run against the transaction-bound {@link GraphTraversalSource}
     * @see #evaluateInTx(Function)
     */
    public void executeInTx(final Consumer<GraphTraversalSource> txWork) {
        evaluateInTx(gtx -> {
            txWork.accept(gtx);
            return null;
        });
    }

    /**
     * Runs the supplied unit of work inside a single transaction, managing the transaction lifecycle automatically,
     * and returns the value the work produces.
     * <p>
     * This wrapper obtains a {@link Transaction} via {@link #tx()}, {@link Transaction#begin() begins} it, invokes
     * {@code txWork} with the transaction-bound {@link GraphTraversalSource} ({@code gtx}), and then
     * {@link Transaction#commit() commits} on normal completion, returning the value computed by {@code txWork}.
     * Error handling:
     * <ul>
     *   <li>If {@code txWork} throws, the transaction is {@link Transaction#rollback() rolled back} and the exact
     *       original error is re-thrown to the caller. If that rollback itself fails, the rollback failure is attached
     *       to the original error via {@link Throwable#addSuppressed(Throwable)} and a warning is logged, but the
     *       original error still propagates as the primary error.</li>
     *   <li>If {@link Transaction#commit() commit} fails, a {@link Transaction#rollback() rollback} is attempted
     *       afterward (to avoid leaving transaction resources tied up on the server), and the commit error is re-thrown
     *       as the primary error. If the follow-up rollback also fails, the rollback failure is attached to the commit
     *       error via {@link Throwable#addSuppressed(Throwable)} and a warning is logged.</li>
     * </ul>
     * <p>
     * This is a <strong>single-shot</strong> operation - exactly one attempt is made, with no automatic retry. The
     * lambda receives the transactional {@code gtx} and should issue its traversals against that source only. Because
     * the lifecycle is driven through {@link #tx()}, the underlying transaction semantics (embedded thread-bound vs.
     * remote server session) are whatever the underlying {@code begin()}/{@code commit()}/{@code rollback()} provide.
     *
     * @param txWork the unit of work to run against the transaction-bound {@link GraphTraversalSource}
     * @param <T> the type of value produced by {@code txWork}
     * @return the value produced by {@code txWork}
     * @see #executeInTx(Consumer)
     */
    public <T> T evaluateInTx(final Function<GraphTraversalSource, T> txWork) {
        final Transaction tx = this.tx();
        final GraphTraversalSource gtx = tx.begin();
        final T result;
        // Phase 1: run the user's work. If it throws, roll back and rethrow the body error - the
        // throw below exits the method, so a failed body never reaches the commit in phase 2.
        try {
            result = txWork.apply(gtx);
        } catch (Throwable bodyError) {
            try {
                tx.rollback();
            } catch (Throwable rollbackError) {
                bodyError.addSuppressed(rollbackError);
                LOGGER.warn("Rollback failed after transaction body error", rollbackError);
            }
            throw bodyError;
        }
        // Phase 2: the body succeeded, so commit. A separate try because this failure mode is
        // distinct (commit, not body): we still roll back for server-side hygiene, then rethrow
        // the commit error as the primary error.
        try {
            tx.commit();
        } catch (Throwable commitError) {
            try {
                tx.rollback();
            } catch (Throwable rollbackError) {
                commitError.addSuppressed(rollbackError);
                LOGGER.warn("Rollback failed after commit failure", rollbackError);
            }
            throw commitError;
        }
        return result;
    }

    /**
     * If there is an underlying {@link RemoteConnection} it will be closed by this method.
     */
    @Override
    public void close() throws Exception {
        if (connection != null) connection.close();
    }

    @Override
    public String toString() {
        return StringFactory.traversalSourceString(this);
    }

}
