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

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SackStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SideEffectStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.TraversalVertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphTraversalSource implements TraversalSource {

    private final Graph graph;
    private TraversalStrategies strategies;
    private boolean oneBulk = false;
    private boolean pathOn = false;

    public GraphTraversalSource(final Graph graph, final TraversalStrategies traversalStrategies) {
        this.graph = graph;
        this.strategies = traversalStrategies;
    }

    public GraphTraversalSource(final Graph graph) {
        this(graph, TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
    }

    private <S> GraphTraversal.Admin<S, S> generateTraversal() {
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(this.graph);
        traversal.setStrategies(this.strategies);
        if (this.oneBulk)
            traversal.addTraverserRequirement(TraverserRequirement.ONE_BULK);
        if (this.pathOn)
            traversal.addTraverserRequirement(TraverserRequirement.PATH);
        return traversal;
    }

    @Override
    public TraversalStrategies getStrategies() {
        return this.strategies;
    }

    @Override
    public Graph getGraph() {
        return this.graph;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public GraphTraversalSource clone() {
        try {
            final GraphTraversalSource clone = (GraphTraversalSource) super.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    //// UTILITIES


    public GraphTraversalSource withComputer(final Function<Graph, GraphComputer> graphComputerFunction) {
        this.strategies = this.strategies.clone();
        this.strategies.addStrategies(new TraversalVertexProgramStrategy(graphComputerFunction), ComputerVerificationStrategy.instance());
        return this;
    }

    public GraphTraversalSource withComputer(final Class<? extends GraphComputer> graphComputerClass) {
        return this.withComputer(g -> g.compute(graphComputerClass));
    }

    public GraphTraversalSource withComputer() {
        return this.withComputer(Graph::compute);
    }

    public GraphTraversalSource withStrategy(final TraversalStrategy... traversalStrategies) {
        this.strategies = this.strategies.clone();
        this.strategies.addStrategies(traversalStrategies);
        return this;
    }

    public GraphTraversalSource withoutStrategy(final Class<? extends TraversalStrategy>... traversalStrategyClass) {
        this.strategies = this.strategies.clone();
        this.strategies.removeStrategies(traversalStrategyClass);
        return this;
    }

    public GraphTraversalSource withSideEffect(final String key, final Supplier sideEffect) {
        this.strategies = this.strategies.clone();
        this.strategies.addStrategies(new SideEffectStrategy(Collections.singletonList(new Pair<>(key, sideEffect))));
        return this;
    }


    public GraphTraversalSource withSideEffect(final Object... keyValues) {
        this.strategies = this.strategies.clone();
        final List<Pair<String, Supplier>> sideEffects = new ArrayList<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            sideEffects.add(new Pair<>((String) keyValues[i], keyValues[i + 1] instanceof Supplier ? (Supplier) keyValues[i + 1] : new ConstantSupplier<>(keyValues[i + 1])));
        }
        this.strategies.addStrategies(new SideEffectStrategy(sideEffects));
        return this;
    }

    public <A> GraphTraversalSource withSack(final A initialValue) {
        return this.withSack(initialValue, null, null);
    }

    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue) {
        return this;//.withSack((Supplier<A>) initialValue, null, null);
    }

    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        return this.withSack(initialValue, splitOperator, null);
    }

    public <A> GraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        return this.withSack(initialValue, splitOperator, null);
    }

    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
        return this.withSack(initialValue, null, mergeOperator);
    }

    public <A> GraphTraversalSource withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
        return this.withSack(initialValue, null, mergeOperator);
    }

    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        this.strategies = this.strategies.clone();
        this.strategies.addStrategies(new SackStrategy(initialValue, splitOperator, mergeOperator));
        return this;
    }

    public <A> GraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        this.strategies = this.strategies.clone();
        this.strategies.addStrategies(new SackStrategy(new ConstantSupplier<>(initialValue), splitOperator, mergeOperator));
        return this;
    }

    public GraphTraversalSource withBulk(final boolean useBulk) {
        this.oneBulk = !useBulk;
        return this;
    }

    public <S> GraphTraversalSource withPath() {
        this.pathOn = true;
        return this;
    }

    /////////////////////////////

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #addV()}
     */
    @Deprecated
    public GraphTraversal<Vertex, Vertex> addV(final Object... keyValues) {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = this.generateTraversal();
        traversal.addStep(new AddVertexStartStep(traversal, null));
        ((AddVertexStartStep) traversal.getEndStep()).addPropertyMutations(keyValues);
        return traversal;
    }

    public GraphTraversal<Vertex, Vertex> addV(final String label) {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = this.generateTraversal();
        return traversal.addStep(new AddVertexStartStep(traversal, label));
    }

    public GraphTraversal<Vertex, Vertex> addV() {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = this.generateTraversal();
        return traversal.addStep(new AddVertexStartStep(traversal, null));
    }

    public <S> GraphTraversal<S, S> inject(S... starts) {
        return (GraphTraversal<S, S>) this.generateTraversal().inject(starts);
    }

    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = this.generateTraversal();
        return traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, vertexIds));
    }

    public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
        final GraphTraversal.Admin<Edge, Edge> traversal = this.generateTraversal();
        return traversal.addStep(new GraphStep<>(traversal, Edge.class, true, edgesIds));
    }


    public Transaction tx() {
        return this.graph.tx();
    }


    @Override
    public String toString() {
        return StringFactory.traversalSourceString(this);
    }
}
