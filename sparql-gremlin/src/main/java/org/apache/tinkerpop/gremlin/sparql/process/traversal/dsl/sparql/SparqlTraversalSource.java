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
package org.apache.tinkerpop.gremlin.sparql.process.traversal.dsl.sparql;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.sparql.process.traversal.strategy.SparqlStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A {@link TraversalSource} implementation that spawns {@link SparqlTraversal} instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SparqlTraversalSource extends GraphTraversalSource {

    public SparqlTraversalSource(final Graph graph, final TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }

    public SparqlTraversalSource(final Graph graph) {
        super(graph);
    }

    public SparqlTraversalSource(final RemoteConnection connection) {
        super(connection);
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public SparqlTraversalSource clone() {
        return (SparqlTraversalSource) super.clone();
    }

    //// CONFIGURATIONS


    @Override
    public SparqlTraversalSource with(final String key) {
        return (SparqlTraversalSource) super.with(key);
    }

    @Override
    public SparqlTraversalSource with(final String key, final Object value) {
        return (SparqlTraversalSource) super.with(key, value);
    }

    @Override
    public SparqlTraversalSource withComputer(final Computer computer) {
        return (SparqlTraversalSource) super.withComputer(computer);
    }

    @Override
    public SparqlTraversalSource withComputer(final Class<? extends GraphComputer> graphComputerClass) {
        return (SparqlTraversalSource) super.withComputer(graphComputerClass);
    }

    @Override
    public SparqlTraversalSource withComputer() {
        return (SparqlTraversalSource) super.withComputer();
    }

    @Override
    public <A> SparqlTraversalSource withSideEffect(final String key, final Supplier<A> initialValue, final BinaryOperator<A> reducer) {
        return (SparqlTraversalSource) super.withSideEffect(key, initialValue, reducer);
    }

    @Override
    public <A> SparqlTraversalSource withSideEffect(final String key, final A initialValue, final BinaryOperator<A> reducer) {
        return (SparqlTraversalSource) super.withSideEffect(key, initialValue, reducer);
    }

    @Override
    public <A> SparqlTraversalSource withSideEffect(final String key, final A initialValue) {
        return (SparqlTraversalSource) super.withSideEffect(key, initialValue);
    }

    @Override
    public <A> SparqlTraversalSource withSideEffect(final String key, final Supplier<A> initialValue) {
        return (SparqlTraversalSource) super.withSideEffect(key, initialValue);
    }

    @Override
    public <A> SparqlTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (SparqlTraversalSource) super.withSack(initialValue, splitOperator, mergeOperator);
    }

    @Override
    public <A> SparqlTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        return (SparqlTraversalSource) super.withSack(initialValue, splitOperator, mergeOperator);
    }

    @Override
    public <A> SparqlTraversalSource withSack(final A initialValue) {
        return (SparqlTraversalSource) super.withSack(initialValue);
    }

    @Override
    public <A> SparqlTraversalSource withSack(final Supplier<A> initialValue) {
        return (SparqlTraversalSource) super.withSack(initialValue);
    }

    @Override
    public <A> SparqlTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        return (SparqlTraversalSource) super.withSack(initialValue, splitOperator);
    }

    @Override
    public <A> SparqlTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        return (SparqlTraversalSource) super.withSack(initialValue, splitOperator);
    }

    @Override
    public <A> SparqlTraversalSource withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
        return (SparqlTraversalSource) super.withSack(initialValue, mergeOperator);
    }

    @Override
    public <A> SparqlTraversalSource withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
        return (SparqlTraversalSource) super.withSack(initialValue, mergeOperator);
    }

    @Override
    public SparqlTraversalSource withBulk(final boolean useBulk) {
        return (SparqlTraversalSource) super.withBulk(useBulk);
    }

    @Override
    public SparqlTraversalSource withPath() {
        return (SparqlTraversalSource) super.withPath();
    }

    @Override
    public SparqlTraversalSource withStrategies(final TraversalStrategy... traversalStrategies) {
        return (SparqlTraversalSource) super.withStrategies(traversalStrategies);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public SparqlTraversalSource withoutStrategies(final Class<? extends TraversalStrategy>... traversalStrategyClasses) {
        return (SparqlTraversalSource) super.withoutStrategies(traversalStrategyClasses);
    }

    /**
     * The start step for a SPARQL based traversal that accepts a string representation of the query to execute.
     */
    public <S> GraphTraversal<S,?> sparql(final String query) {
        final SparqlTraversalSource clone = this.withStrategies(SparqlStrategy.instance()).clone();

        // the inject() holds the sparql which the SparqlStrategy then detects and converts to a traversal
        clone.gremlinLang.addStep(GraphTraversal.Symbols.inject, query);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal(clone);
        return traversal.addStep(new InjectStep<>(traversal, query));
    }
}
