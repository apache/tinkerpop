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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A {@code GraphTraversalSource} is the primary DSL of the Gremlin traversal machine.
 * It provides access to all the configurations and steps for Turing complete graph computing.
 * Any DSL can be constructed based on the methods of both {@code GraphTraversalSource} and {@link GraphTraversal}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphTraversalSource implements TraversalSource {

    private final Graph graph;
    private TraversalStrategies strategies;

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
            clone.strategies = this.strategies.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    //// CONFIGURATIONS

    @Override
    public GraphTraversalSource withComputer(final Computer computer) {
        return (GraphTraversalSource) TraversalSource.super.withComputer(computer);
    }

    @Override
    public GraphTraversalSource withComputer(final Class<? extends GraphComputer> graphComputerClass) {
        return (GraphTraversalSource) TraversalSource.super.withComputer(graphComputerClass);
    }

    @Override
    public GraphTraversalSource withComputer() {
        return (GraphTraversalSource) TraversalSource.super.withComputer();
    }

    @Override
    public GraphTraversalSource withStrategies(final TraversalStrategy... traversalStrategies) {
        return (GraphTraversalSource) TraversalSource.super.withStrategies(traversalStrategies);
    }

    @Override
    @SuppressWarnings({"unchecked", "varargs"})
    public GraphTraversalSource withoutStrategies(final Class<? extends TraversalStrategy>... traversalStrategyClasses) {
        return (GraphTraversalSource) TraversalSource.super.withoutStrategies(traversalStrategyClasses);
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue, final BinaryOperator<A> reducer) {
        return (GraphTraversalSource) TraversalSource.super.withSideEffect(key, initialValue, reducer);
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final A initialValue, final BinaryOperator<A> reducer) {
        return (GraphTraversalSource) TraversalSource.super.withSideEffect(key, initialValue, reducer);
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final A initialValue) {
        return (GraphTraversalSource) TraversalSource.super.withSideEffect(key, initialValue);
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue) {
        return (GraphTraversalSource) TraversalSource.super.withSideEffect(key, initialValue);
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, splitOperator, mergeOperator);
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, splitOperator, mergeOperator);
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue);
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue);
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, splitOperator);
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, splitOperator);
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, mergeOperator);
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
        return (GraphTraversalSource) TraversalSource.super.withSack(initialValue, mergeOperator);
    }

    public GraphTraversalSource withBulk(final boolean useBulk) {
        if (!useBulk) {
            final GraphTraversalSource clone = this.clone();
            RequirementsStrategy.addRequirements(clone.strategies, TraverserRequirement.ONE_BULK);
            return clone;
        } else {
            return this;
        }
    }

    public GraphTraversalSource withPath() {
        final GraphTraversalSource clone = this.clone();
        RequirementsStrategy.addRequirements(clone.strategies, TraverserRequirement.PATH);
        return clone;
    }

    //// SPAWNS

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

    //////////////////
    // DEPRECATION //
    /////////////////

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    public static Builder build() {
        return new Builder();
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    public static Builder standard() {
        return GraphTraversalSource.build().engine(StandardTraversalEngine.build());
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    public static Builder computer() {
        return GraphTraversalSource.build().engine(ComputerTraversalEngine.build());
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    public static Builder computer(final Class<? extends GraphComputer> graphComputerClass) {
        return GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(graphComputerClass));
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    public final static class Builder implements TraversalSource.Builder<GraphTraversalSource> {

        private TraversalEngine.Builder engineBuilder = StandardTraversalEngine.build();
        private List<TraversalStrategy> withStrategies = new ArrayList<>();
        private List<Class<? extends TraversalStrategy>> withoutStrategies = new ArrayList<>();

        private Builder() {
        }

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        @Override
        public Builder engine(final TraversalEngine.Builder engineBuilder) {
            this.engineBuilder = engineBuilder;
            return this;
        }

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        @Override
        public Builder with(final TraversalStrategy strategy) {
            this.withStrategies.add(strategy);
            return this;
        }

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        @Override
        public TraversalSource.Builder without(final Class<? extends TraversalStrategy> strategyClass) {
            this.withoutStrategies.add(strategyClass);
            return this;
        }

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        @Override
        public GraphTraversalSource create(final Graph graph) {
            GraphTraversalSource traversalSource = new GraphTraversalSource(graph);
            if (!this.withStrategies.isEmpty())
                traversalSource = traversalSource.withStrategies(this.withStrategies.toArray(new TraversalStrategy[this.withStrategies.size()]));
            if (!this.withoutStrategies.isEmpty())
                traversalSource = traversalSource.withoutStrategies(this.withoutStrategies.toArray(new Class[this.withoutStrategies.size()]));
            if (this.engineBuilder instanceof ComputerTraversalEngine.Builder)
                traversalSource = (GraphTraversalSource) ((ComputerTraversalEngine.Builder) this.engineBuilder).create(traversalSource);
            return traversalSource;
        }
    }
}
