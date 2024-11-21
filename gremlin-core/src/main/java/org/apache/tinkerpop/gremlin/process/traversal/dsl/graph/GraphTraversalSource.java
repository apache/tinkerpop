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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
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
        return traversal.addStep(new AddVertexStartStep(traversal, vertexLabel));
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
        return traversal.addStep(new AddVertexStartStep(traversal, vertexLabelTraversal));
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
        return traversal.addStep(new AddVertexStartStep(traversal, (String) null));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the specified label. If the {@code label} is
     * {@code null} then it will default to {@link Vertex#DEFAULT_LABEL}.
     *
     * @since 4.0.0
     */
    public GraphTraversal<Vertex, Vertex> addV(final GValue<String> vertexLabel) {
        if (null == vertexLabel) throw new IllegalArgumentException("vertexLabel cannot be null");
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.addV, vertexLabel);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddVertexStartStep(traversal, vertexLabel));
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
        return traversal.addStep(new AddEdgeStartStep(traversal, label));
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
        return traversal.addStep(new AddEdgeStartStep(traversal, edgeLabelTraversal));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding an edge with the specified label.
     *
     * @since 4.0.0
     */
    public GraphTraversal<Edge, Edge> addE(final GValue<String> label) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.addE, label);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddEdgeStartStep(traversal, label));
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
        return traversal.addStep(new MergeVertexStep(traversal, true, searchCreate));
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

        final MergeVertexStep<S> step = null == searchCreate ? new MergeVertexStep(traversal, true, (Map) null) :
                new MergeVertexStep(traversal, true, searchCreate.asAdmin());

        return traversal.addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Vertex} using a
     * {@code Map} as an argument. The {@code Map} represents search criteria and will match each of the supplied
     * key/value pairs where the keys may be {@code String} property values or a value of {@link T}. If a match is not
     * made it will use that search criteria to create the new {@link Vertex}.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} or a {@code String}.
     * @since 4.0.0
     */
    public GraphTraversal<Vertex, Vertex> mergeV(final GValue<Map<Object, Object>> searchCreate) {
        final GraphTraversalSource clone = GraphTraversalSource.this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.mergeV, searchCreate);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new MergeVertexStep(traversal, true, searchCreate));
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Edge} using a
     * {@code Map} as an argument.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} {@link Direction} or a {@code String}.
     * @since 3.6.0
     */
    public GraphTraversal<Edge, Edge> mergeE(final Map<?, Object> searchCreate) {
        final GraphTraversalSource clone = this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.mergeE, searchCreate);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new MergeEdgeStep(traversal, true, searchCreate));
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

        final MergeEdgeStep step = null == searchCreate ? new MergeEdgeStep(traversal, true,  (Map) null) :
                new MergeEdgeStep(traversal, true, searchCreate.asAdmin());

        return traversal.addStep(step);
    }

    /**
     * Spawns a {@link GraphTraversal} by doing a merge (i.e. upsert) style operation for an {@link Edge} using a
     * {@code Map} as an argument.
     *
     * @param searchCreate This {@code Map} can have a key of {@link T} {@link Direction} or a {@code String}.
     * @since 4.0.0
     */
    public GraphTraversal<Edge, Edge> mergeE(final GValue<Map<?, Object>> searchCreate) {
        final GraphTraversalSource clone = GraphTraversalSource.this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.mergeE, searchCreate);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new MergeEdgeStep(traversal, true, searchCreate));
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
        return traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, ids));
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
        return traversal.addStep(new GraphStep<>(traversal, Edge.class, true, ids));
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
     * @since 4.0.0
     */
    public <S> GraphTraversal<S, S> call(final String service, final GValue<Map> params) {
        final GraphTraversalSource clone = GraphTraversalSource.this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.call, service, params);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new CallStep<>(traversal, true, service, params));
    }

    /**
     * Spawns a {@link GraphTraversal} starting with values produced by the specified service call with the specified
     * static parameters.
     *
     * @param service the name of the service call
     * @param params static parameter map (no nested traversals)
     * @since 4.0.0
     */
    public <S> GraphTraversal<S, S> call(final String service, final GValue<Map> params, final Traversal<S, Map> childTraversal) {
        final GraphTraversalSource clone = GraphTraversalSource.this.clone();
        clone.gremlinLang.addStep(GraphTraversal.Symbols.call, service, params, childTraversal);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new CallStep<>(traversal, true, service, params, childTraversal.asAdmin()));
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
     */
    public Transaction tx() {
        if (null == this.connection)
            return this.graph.tx();
        else {
            throw new UnsupportedOperationException("TinkerPop 4 does not yet support remote transactions");
        }

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
