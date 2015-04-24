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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.PathIdentityStep;
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
    private final TraversalEngine.Builder engine;
    private final TraversalStrategies strategies;
    private final List<TraversalStrategy> withStrategies;
    private final List<Class<? extends TraversalStrategy>> withoutStrategies;

    private GraphTraversalSource(final Graph graph, final TraversalEngine.Builder engine, final List<TraversalStrategy> withStrategies, final List<Class<? extends TraversalStrategy>> withoutStrategies) {
        this.graph = graph;
        this.engine = engine;
        this.withStrategies = withStrategies;
        this.withoutStrategies = withoutStrategies;
        final TraversalStrategies tempStrategies = TraversalStrategies.GlobalCache.getStrategies(TraversalStrategies.GlobalCache.getGraphClass(this.graph));
        this.strategies = withStrategies.isEmpty() && withoutStrategies.isEmpty() ?
                tempStrategies :
                tempStrategies.clone()
                        .addStrategies(withStrategies.toArray(new TraversalStrategy[withStrategies.size()]))
                        .removeStrategies(withoutStrategies.toArray(new Class[withoutStrategies.size()]));
    }

    private <S> GraphTraversal.Admin<S, S> generateTraversal() {
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(this.graph);
        traversal.setEngine(this.engine.create(this.graph));
        traversal.setStrategies(this.strategies);
        return traversal;
    }

    public GraphTraversal<Vertex, Vertex> addV(final Object... keyValues) {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = this.generateTraversal();
        return traversal.addStep(new AddVertexStartStep(traversal, keyValues));
    }

    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        final GraphTraversal.Admin<Vertex, Vertex> traversal = this.generateTraversal();
        return traversal.addStep(new GraphStep<>(traversal, Vertex.class, vertexIds));
    }

    public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
        final GraphTraversal.Admin<Edge, Edge> traversal = this.generateTraversal();
        return traversal.addStep(new GraphStep<>(traversal, Edge.class, edgesIds));
    }

    //// UTILITIES

    public <S> GraphTraversalSourceStub withSideEffect(final String key, final Supplier supplier) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.getSideEffects().registerSupplier(key, supplier);
        return new GraphTraversalSourceStub(traversal, false);
    }

    public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.getSideEffects().setSack(initialValue, Optional.of(splitOperator));
        return new GraphTraversalSourceStub(traversal, false);
    }

    public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.getSideEffects().setSack(initialValue, Optional.empty());
        return new GraphTraversalSourceStub(traversal, false);
    }

    public <A> GraphTraversalSourceStub withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.getSideEffects().setSack(new ConstantSupplier<>(initialValue), Optional.of(splitOperator));
        return new GraphTraversalSourceStub(traversal, false);
    }

    public <A> GraphTraversalSourceStub withSack(final A initialValue) {
        final GraphTraversal.Admin traversal = this.generateTraversal();
        traversal.getSideEffects().setSack(new ConstantSupplier<>(initialValue), Optional.empty());
        return new GraphTraversalSourceStub(traversal, false);
    }

    public <S> GraphTraversalSourceStub withPath() {
        return new GraphTraversalSourceStub(this.generateTraversal(), true);
    }

    public Transaction tx() {
        return this.graph.tx();
    }

    public static Builder build() {
        return new Builder();
    }


    @Override
    public Optional<GraphComputer> getGraphComputer() {
        return this.engine.create(this.graph).getGraphComputer();
    }

    @Override
    public Optional<Graph> getGraph() {
        return Optional.ofNullable(this.graph);
    }

    @Override
    public GraphTraversalSource.Builder asBuilder() {
        final GraphTraversalSource.Builder builder = GraphTraversalSource.build().engine(this.engine);
        this.withStrategies.forEach(builder::with);
        this.withoutStrategies.forEach(builder::without);
        return builder;
    }

    @Override
    public String toString() {
        return StringFactory.traversalSourceString(this);
    }

    //////

    public static class Builder implements TraversalSource.Builder<GraphTraversalSource> {

        private TraversalEngine.Builder engineBuilder = StandardTraversalEngine.build();
        private List<TraversalStrategy> withStrategies = null;
        private List<Class<? extends TraversalStrategy>> withoutStrategies = null;

        private Builder() {
        }

        @Override
        public Builder engine(final TraversalEngine.Builder engineBuilder) {
            this.engineBuilder = engineBuilder;
            return this;
        }

        @Override
        public Builder with(final TraversalStrategy strategy) {
            if (null == this.withStrategies) this.withStrategies = new ArrayList<>();
            this.withStrategies.add(strategy);
            return this;
        }

        @Override
        public TraversalSource.Builder without(Class<? extends TraversalStrategy> strategyClass) {
            if (null == this.withoutStrategies) this.withoutStrategies = new ArrayList<>();
            this.withoutStrategies.add(strategyClass);
            return this;
        }

        @Override
        public GraphTraversalSource create(final Graph graph) {
            return new GraphTraversalSource(graph, this.engineBuilder,
                    null == this.withStrategies ? Collections.emptyList() : this.withStrategies,
                    null == this.withoutStrategies ? Collections.emptyList() : this.withoutStrategies);
        }
    }

    public static class GraphTraversalSourceStub {

        private final GraphTraversal.Admin traversal;
        private boolean withPaths;

        public GraphTraversalSourceStub(final GraphTraversal.Admin traversal, final boolean withPaths) {
            this.traversal = traversal;
            this.withPaths = withPaths;
        }

        public GraphTraversal<Vertex, Vertex> addV(final Object... keyValues) {
            this.traversal.addStep(new AddVertexStartStep(this.traversal, keyValues));
            return ((this.withPaths) ? this.traversal.addStep(new PathIdentityStep<>(this.traversal)) : this.traversal);
        }

        public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
            this.traversal.addStep(new GraphStep<>(this.traversal, Vertex.class, vertexIds));
            return ((this.withPaths) ? this.traversal.addStep(new PathIdentityStep<>(this.traversal)) : this.traversal);
        }

        public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
            this.traversal.addStep(new GraphStep<>(this.traversal, Edge.class, edgesIds));
            return ((this.withPaths) ? this.traversal.addStep(new PathIdentityStep<>(this.traversal)) : this.traversal);
        }

        //// UTILITIES

        public GraphTraversalSourceStub withSideEffect(final String key, final Supplier supplier) {
            this.traversal.getSideEffects().registerSupplier(key, supplier);
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
            this.traversal.getSideEffects().setSack(initialValue, Optional.of(splitOperator));
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final Supplier<A> initialValue) {
            this.traversal.getSideEffects().setSack(initialValue, Optional.empty());
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
            this.traversal.getSideEffects().setSack(new ConstantSupplier<>(initialValue), Optional.of(splitOperator));
            return this;
        }

        public <A> GraphTraversalSourceStub withSack(final A initialValue) {
            this.traversal.getSideEffects().setSack(new ConstantSupplier<>(initialValue), Optional.empty());
            return this;
        }

        public GraphTraversalSourceStub withPath() {
            this.withPaths = true;
            return this;
        }
    }
}
