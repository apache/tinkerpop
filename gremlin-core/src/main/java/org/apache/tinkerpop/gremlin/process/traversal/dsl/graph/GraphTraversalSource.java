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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphTraversalSource implements TraversalSource {

    public static Builder standard() {
        return GraphTraversalSource.build().engine(StandardTraversalEngine.build());
    }

    public static Builder computer() {
        return GraphTraversalSource.build().engine(ComputerTraversalEngine.build());
    }

    public static Builder computer(final Class<? extends GraphComputer> graphComputerClass) {
        return GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(graphComputerClass));
    }

    ////

    private final transient Graph graph;
    private final TraversalEngine.Builder engineBuilder;
    private final TraversalStrategies strategies;
    private final List<TraversalStrategy> withStrategies;
    private final List<Class<? extends TraversalStrategy>> withoutStrategies;

    private GraphTraversalSource(final Graph graph, final TraversalEngine.Builder engineBuilder, final List<TraversalStrategy> withStrategies, final List<Class<? extends TraversalStrategy>> withoutStrategies) {
        this.graph = graph;
        this.engineBuilder = engineBuilder;
        this.withStrategies = withStrategies;
        this.withoutStrategies = withoutStrategies;
        this.withStrategies.addAll(engineBuilder.getWithStrategies());
        this.withoutStrategies.addAll(engineBuilder.getWithoutStrategies());
        final TraversalStrategies tempStrategies = TraversalStrategies.GlobalCache.getStrategies(this.graph.getClass());
        this.strategies = withStrategies.isEmpty() && withoutStrategies.isEmpty() ?
                tempStrategies :
                tempStrategies.clone()
                        .addStrategies(withStrategies.toArray(new TraversalStrategy[withStrategies.size()]))
                        .removeStrategies(withoutStrategies.toArray(new Class[withoutStrategies.size()]));
    }

    private <S> GraphTraversal.Admin<S, S> generateTraversal() {
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(this.graph);
        final TraversalEngine engine = this.engineBuilder.create(this.graph);
        traversal.setEngine(engine);
        traversal.setStrategies(this.strategies);
        return traversal;
    }

    public <S> GraphTraversal<S, S> inject(S... starts) {
        return (GraphTraversal<S, S>) this.generateTraversal().inject(starts);
    }

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

    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = this.generateTraversal();
        return traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, vertexIds));
    }

    public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
        final GraphTraversal.Admin<Edge, Edge> traversal = this.generateTraversal();
        return traversal.addStep(new GraphStep<>(traversal, Edge.class, true, edgesIds));
    }

    //// UTILITIES

    public GraphTraversalSourceStub withSideEffect(final String key, final Supplier supplier) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.getSideEffects().registerSupplier(key, supplier);
        return new GraphTraversalSourceStub(traversal);
    }

    public GraphTraversalSourceStub withSideEffect(final String key, final Object object) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.getSideEffects().registerSupplier(key, new ConstantSupplier<>(object));
        return new GraphTraversalSourceStub(traversal);
    }

    public <A> GraphTraversalSourceStub withSack(final A initialValue) {
        return this.withSack(initialValue, null, null);
    }

    public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.getSideEffects().setSack(initialValue, null, null);
        return new GraphTraversalSourceStub(traversal);
    }

    public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        return this.withSack(initialValue, splitOperator, null);
    }

    public <A> GraphTraversalSourceStub withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        return this.withSack(initialValue, splitOperator, null);
    }

    public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
        return this.withSack(initialValue, null, mergeOperator);
    }

    public <A> GraphTraversalSourceStub withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
        return this.withSack(initialValue, null, mergeOperator);
    }

    public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.getSideEffects().setSack(initialValue, splitOperator, mergeOperator);
        return new GraphTraversalSourceStub(traversal);
    }

    public <A> GraphTraversalSourceStub withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.getSideEffects().setSack(new ConstantSupplier<>(initialValue), splitOperator, mergeOperator);
        return new GraphTraversalSourceStub(traversal);
    }

    public GraphTraversalSourceStub withBulk(final boolean useBulk) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.addTraverserRequirement(useBulk ? TraverserRequirement.BULK : TraverserRequirement.ONE_BULK);
        return new GraphTraversalSourceStub(traversal);
    }

    public <S> GraphTraversalSourceStub withPath() {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.addTraverserRequirement(TraverserRequirement.PATH);
        return new GraphTraversalSourceStub(traversal);
    }

    public Transaction tx() {
        return this.graph.tx();
    }

    public static Builder build() {
        return new Builder();
    }

    @Override
    public List<TraversalStrategy> getStrategies() {
        return Collections.unmodifiableList(this.strategies.toList());
    }

    @Override
    public Optional<GraphComputer> getGraphComputer() {
        return this.engineBuilder.create(this.graph).getGraphComputer();
    }

    @Override
    public Optional<Graph> getGraph() {
        return Optional.ofNullable(this.graph);
    }

    @Override
    public GraphTraversalSource.Builder asBuilder() {
        final GraphTraversalSource.Builder builder = GraphTraversalSource.build().engine(this.engineBuilder);
        this.withStrategies.forEach(builder::with);
        this.withoutStrategies.forEach(builder::without);
        return builder;
    }

    @Override
    public String toString() {
        return StringFactory.traversalSourceString(this);
    }

    //////

    public final static class Builder implements TraversalSource.Builder<GraphTraversalSource> {

        private TraversalEngine.Builder engineBuilder = StandardTraversalEngine.build();
        private List<TraversalStrategy> withStrategies = new ArrayList<>();
        private List<Class<? extends TraversalStrategy>> withoutStrategies = new ArrayList<>();

        private Builder() {
        }

        @Override
        public Builder engine(final TraversalEngine.Builder engineBuilder) {
            this.engineBuilder = engineBuilder;
            return this;
        }

        @Override
        public Builder with(final TraversalStrategy strategy) {
            this.withStrategies.add(strategy);
            return this;
        }

        @Override
        public TraversalSource.Builder without(final Class<? extends TraversalStrategy> strategyClass) {
            this.withoutStrategies.add(strategyClass);
            return this;
        }

        @Override
        public GraphTraversalSource create(final Graph graph) {
            return new GraphTraversalSource(graph, this.engineBuilder, this.withStrategies, this.withoutStrategies);
        }
    }

    public static class GraphTraversalSourceStub {

        private final GraphTraversal.Admin traversal;

        public GraphTraversalSourceStub(final GraphTraversal.Admin traversal) {
            this.traversal = traversal;
        }

        public <S> GraphTraversal<S, S> inject(S... starts) {
            return this.traversal.inject(starts);
        }

        /**
         * @deprecated As of release 3.1.0, replaced by {@link #addV()}
         */
        @Deprecated
        public GraphTraversal<Vertex, Vertex> addV(final Object... keyValues) {
            this.traversal.addStep(new AddVertexStartStep(this.traversal, null));
            ((AddVertexStartStep) this.traversal.getEndStep()).addPropertyMutations(keyValues);
            return this.traversal;
        }

        public GraphTraversal<Vertex, Vertex> addV(final String label) {
            return this.traversal.addStep(new AddVertexStartStep(this.traversal, label));
        }

        public GraphTraversal<Vertex, Vertex> addV() {
            return this.traversal.addStep(new AddVertexStartStep(this.traversal, null));
        }

        public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
            return this.traversal.addStep(new GraphStep<>(this.traversal, Vertex.class, true, vertexIds));
        }

        public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
            return this.traversal.addStep(new GraphStep<>(this.traversal, Edge.class, true, edgesIds));
        }

        //// UTILITIES

        public GraphTraversalSourceStub withSideEffect(final String key, final Supplier supplier) {
            this.traversal.getSideEffects().registerSupplier(key, supplier);
            return this;
        }

        public GraphTraversalSourceStub withSideEffect(final String key, final Object object) {
            this.traversal.getSideEffects().registerSupplier(key, new ConstantSupplier<>(object));
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final A initialValue) {
            this.traversal.getSideEffects().setSack(new ConstantSupplier<>(initialValue), null, null);
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue) {
            this.traversal.getSideEffects().setSack(initialValue, null, null);
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
            this.traversal.getSideEffects().setSack(initialValue, splitOperator, null);
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
            this.traversal.getSideEffects().setSack(new ConstantSupplier<>(initialValue), splitOperator, null);
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
            this.traversal.getSideEffects().setSack(initialValue, null, mergeOperator);
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
            this.traversal.getSideEffects().setSack(new ConstantSupplier<>(initialValue), null, mergeOperator);
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
            this.traversal.getSideEffects().setSack(initialValue, splitOperator, mergeOperator);
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
            this.traversal.getSideEffects().setSack(new ConstantSupplier<>(initialValue), splitOperator, mergeOperator);
            return this;
        }

        public GraphTraversalSourceStub withBulk(final boolean useBulk) {
            this.traversal.addTraverserRequirement(useBulk ? TraverserRequirement.BULK : TraverserRequirement.ONE_BULK);
            return this;
        }

        public GraphTraversalSourceStub withPath() {
            this.traversal.addTraverserRequirement(TraverserRequirement.PATH);
            return this;
        }
    }
}
