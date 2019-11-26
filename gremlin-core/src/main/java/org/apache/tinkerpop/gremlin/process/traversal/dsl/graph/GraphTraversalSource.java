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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

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
    protected Bytecode bytecode = new Bytecode();

    ////////////////

    public static final class Symbols {

        private Symbols() {
            // static fields only
        }

        public static final String withBulk = "withBulk";
        public static final String withPath = "withPath";

    }

    ////////////////

    @Override
    public Optional<Class> getAnonymousTraversalClass() {
        return Optional.of(__.class);
    }

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
    public TraversalStrategies getStrategies() {
        return this.strategies;
    }

    @Override
    public Graph getGraph() {
        return this.graph;
    }

    @Override
    public Bytecode getBytecode() {
        return this.bytecode;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public GraphTraversalSource clone() {
        try {
            final GraphTraversalSource clone = (GraphTraversalSource) super.clone();
            clone.strategies = this.strategies.clone();
            clone.bytecode = this.bytecode.clone();
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
        clone.bytecode.addSource(Symbols.withBulk, useBulk);
        return clone;
    }

    public GraphTraversalSource withPath() {
        final GraphTraversalSource clone = this.clone();
        RequirementsStrategy.addRequirements(clone.getStrategies(), TraverserRequirement.PATH);
        clone.bytecode.addSource(Symbols.withPath);
        return clone;
    }

    //// SPAWNS


    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the specified label. If the {@code label} is
     * {@code null} then it will default to {@link Vertex#DEFAULT_LABEL}.
     */
    public GraphTraversal<Vertex, Vertex> addV(final String label) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.addV, label);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddVertexStartStep(traversal, label));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the label as determined by a {@link Traversal}. If the
     * {@code vertexLabelTraversal} is {@code null} then it will default to {@link Vertex#DEFAULT_LABEL}.
     */
    public GraphTraversal<Vertex, Vertex> addV(final Traversal<?, String> vertexLabelTraversal) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.addV, vertexLabelTraversal);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddVertexStartStep(traversal, vertexLabelTraversal));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a vertex with the default label.
     */
    public GraphTraversal<Vertex, Vertex> addV() {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.addV);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddVertexStartStep(traversal, (String)null));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a edge with the specified label.
     */
    public GraphTraversal<Edge, Edge> addE(final String label) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.addE, label);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddEdgeStartStep(traversal, label));
    }

    /**
     * Spawns a {@link GraphTraversal} by adding a edge with a label as specified by the provided {@link Traversal}.
     */
    public GraphTraversal<Edge, Edge> addE(final Traversal<?, String> edgeLabelTraversal) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.addE, edgeLabelTraversal);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddEdgeStartStep(traversal, edgeLabelTraversal));
    }

    /**
     * Spawns a {@link GraphTraversal} starting it with arbitrary values.
     */
    public <S> GraphTraversal<S, S> inject(S... starts) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.inject, starts);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new InjectStep<S>(traversal, starts));
    }

    /**
     * Spawns a {@link GraphTraversal} starting with all vertices or some subset of vertices as specified by their
     * unique identifier.
     */
    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.V, vertexIds);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, vertexIds));
    }

    /**
     * Spawns a {@link GraphTraversal} starting with all edges or some subset of edges as specified by their unique
     * identifier.
     */
    public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.E, edgesIds);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new GraphStep<>(traversal, Edge.class, true, edgesIds));
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
        clone.bytecode.addStep(GraphTraversal.Symbols.io, file);
        final GraphTraversal.Admin<S,S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new IoStep<S>(traversal, file));
    }

    /**
     * Proxies calls through to the underlying {@link Graph#tx()}.
     */
    public Transaction tx() {
        return this.graph.tx();
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
