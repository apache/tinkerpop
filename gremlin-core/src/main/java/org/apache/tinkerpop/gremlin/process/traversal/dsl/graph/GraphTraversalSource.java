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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.List;
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

    @Override
    public GraphTraversalSource withStrategies(final TraversalStrategy... traversalStrategies) {
        return (GraphTraversalSource) TraversalSource.super.withStrategies(traversalStrategies);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public GraphTraversalSource withoutStrategies(final Class<? extends TraversalStrategy>... traversalStrategyClasses) {
        return (GraphTraversalSource) TraversalSource.super.withoutStrategies(traversalStrategyClasses);
    }

    @Override
    @Deprecated
    public GraphTraversalSource withBindings(final Bindings bindings) {
        return (GraphTraversalSource) TraversalSource.super.withBindings(bindings);
    }

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

    @Override
    public GraphTraversalSource withRemote(final Configuration conf) {
        return (GraphTraversalSource) TraversalSource.super.withRemote(conf);
    }

    @Override
    public GraphTraversalSource withRemote(final String configFile) throws Exception {
        return (GraphTraversalSource) TraversalSource.super.withRemote(configFile);
    }

    @Override
    public GraphTraversalSource withRemote(final RemoteConnection connection) {
        try {
            // check if someone called withRemote() more than once, so just release resources on the initial
            // connection as you can't have more than one. maybe better to toss IllegalStateException??
            if (this.connection != null)
                this.connection.close();
        } catch (Exception ignored) {
            // not sure there's anything to do here
        }

        this.connection = connection;
        final TraversalSource clone = this.clone();
        clone.getStrategies().addStrategies(new RemoteStrategy(connection));
        return (GraphTraversalSource) clone;
    }

    //// SPAWNS

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #addV()}
     */
    @Deprecated
    public GraphTraversal<Vertex, Vertex> addV(final Object... keyValues) {
        if (keyValues.length != 0 && keyValues[0].equals(T.label)) {
            final GraphTraversal<Vertex, Vertex> traversal = this.addV(keyValues[1].toString());
            this.addV(keyValues[1].toString());
            for (int i = 2; i < keyValues.length; i = i + 2) {
                traversal.property(keyValues[i], keyValues[i + 1]);
            }
            return traversal;
        } else {
            final GraphTraversal<Vertex, Vertex> traversal = this.addV();
            this.addV(keyValues[1].toString());
            for (int i = 0; i < keyValues.length; i = i + 2) {
                traversal.property(keyValues[i], keyValues[i + 1]);
            }
            return traversal;
        }
    }

    public GraphTraversal<Vertex, Vertex> addV(final String label) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.addV, label);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddVertexStartStep(traversal, label));
    }

    public GraphTraversal<Vertex, Vertex> addV() {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.addV);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new AddVertexStartStep(traversal, null));
    }

    public <S> GraphTraversal<S, S> inject(S... starts) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.inject, starts);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new InjectStep<S>(traversal, starts));
    }

    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.V, vertexIds);
        final GraphTraversal.Admin<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new GraphStep<>(traversal, Vertex.class, true, vertexIds));
    }

    public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
        final GraphTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.E, edgesIds);
        final GraphTraversal.Admin<Edge, Edge> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new GraphStep<>(traversal, Edge.class, true, edgesIds));
    }


    public Transaction tx() {
        return this.graph.tx();
    }

    @Override
    public void close() throws Exception {
        if (connection != null) connection.close();
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
