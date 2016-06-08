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

package org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.script;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.Translator;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptGraphTraversalSource extends GraphTraversalSource {

    private final Translator<GraphTraversal> translator;

    public ScriptGraphTraversalSource(final Graph graph, final TraversalStrategies traversalStrategies, final Translator<GraphTraversal> translator) {
        super(graph, traversalStrategies);
        this.translator = translator;
    }

    public ScriptGraphTraversalSource(final Graph graph, final Translator<GraphTraversal> translator) {
        super(graph);
        this.translator = translator;
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
    public GraphTraversalSource clone() {
        final ScriptGraphTraversalSource clone = (ScriptGraphTraversalSource) super.clone();
        clone.strategies = this.strategies.clone();
        return clone;
    }


    //// CONFIGURATIONS

    @Override
    public GraphTraversalSource withComputer(final Computer computer) {
        this.translator.addStrategy("withComputer", computer);
        return this;
    }

    @Override
    public GraphTraversalSource withComputer(final Class<? extends GraphComputer> graphComputerClass) {
        this.translator.addStrategy("withComputer", graphComputerClass);
        return this;
    }

    @Override
    public GraphTraversalSource withComputer() {
        this.translator.addStrategy("withComputer");
        return this;
    }

    @Override
    public GraphTraversalSource withStrategies(final TraversalStrategy... traversalStrategies) {
        return super.withStrategies(traversalStrategies);
        //this.translator.addStep("withStrategies", traversalStrategies);
    }

    @Override
    @SuppressWarnings({"unchecked", "varargs"})
    public GraphTraversalSource withoutStrategies(final Class<? extends TraversalStrategy>... traversalStrategyClasses) {
        this.translator.addStrategy("withoutStrategies", traversalStrategyClasses);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue, final BinaryOperator<A> reducer) {
        this.translator.addStrategy("withSideEffect", key, initialValue, reducer);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final A initialValue, final BinaryOperator<A> reducer) {
        this.translator.addStrategy("withSideEffect", key, initialValue, reducer);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final A initialValue) {
        this.translator.addStrategy("withSideEffect", key, initialValue);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue) {
        this.translator.addStrategy("withSideEffect", key, initialValue);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        this.translator.addStrategy("withSack", initialValue, splitOperator, mergeOperator);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        this.translator.addStrategy("withSack", initialValue, splitOperator, mergeOperator);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue) {
        this.translator.addStrategy("withSack", initialValue);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue) {
        this.translator.addStrategy("withSack", initialValue);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        this.translator.addStrategy("withSack", initialValue, splitOperator);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        this.translator.addStrategy("withSack", initialValue, splitOperator);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
        this.translator.addStrategy("withSack", initialValue, mergeOperator);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
        this.translator.addStrategy("withSack", initialValue, mergeOperator);
        return this;
    }

    public GraphTraversalSource withBulk(final boolean useBulk) {
        this.translator.addStrategy("withBulk", useBulk);
        return this;
    }

    public GraphTraversalSource withPath() {
        this.translator.addStrategy("withPath");
        return this;
    }

    //// SPAWNS

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #addV()}
     */
    @Deprecated
    public GraphTraversal<Vertex, Vertex> addV(final Object... keyValues) {
        return this.generateTraversal("addV", keyValues);
    }

    public GraphTraversal<Vertex, Vertex> addV(final String label) {
        return this.generateTraversal("addV", label);
    }

    public GraphTraversal<Vertex, Vertex> addV() {
        return this.generateTraversal("addV");
    }

    public <S> GraphTraversal<S, S> inject(S... starts) {
        return this.generateTraversal("inject", starts);
    }

    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        return this.generateTraversal("V", vertexIds);
    }

    public GraphTraversal<Edge, Edge> E(final Object... edgesIds) {
        return this.generateTraversal("E", edgesIds);
    }

    private GraphTraversal generateTraversal(final String stepName, final Object... arguments) {
        __.setAnonymousGraphTraversalSupplier(this.translator::__);
        final Translator clone = this.translator.clone();
        clone.addStep(stepName, arguments);
        final GraphTraversal traversal = new ScriptGraphTraversal(this.graph, clone);
        traversal.asAdmin().setStrategies(this.strategies);
        return traversal;
    }


    public Transaction tx() {
        return this.graph.tx();
    }


    @Override
    public String toString() {
        return StringFactory.traversalSourceString(this);
    }
}
