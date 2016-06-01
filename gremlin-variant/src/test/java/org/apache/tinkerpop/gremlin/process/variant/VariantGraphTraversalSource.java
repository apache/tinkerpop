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

package org.apache.tinkerpop.gremlin.process.variant;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VariantGraphTraversalSource extends GraphTraversalSource {

    private VariantConverter variantConverter;
    private StringBuilder sourceString = new StringBuilder("g");

    public VariantGraphTraversalSource(final VariantConverter variantConverter, final Graph graph, final TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
        this.variantConverter = variantConverter;
    }

    private static String getMethodName() {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    ///

    @Override
    public GraphTraversal<Edge, Edge> E(final Object... edgeIds) {
        final StringBuilder temp = new StringBuilder(this.sourceString.toString());
        this.variantConverter.addStep(temp, getMethodName(), edgeIds);
        return new VariantGraphTraversal<>(this.getGraph(), temp, this.variantConverter);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        final StringBuilder temp = new StringBuilder(this.sourceString.toString());
        this.variantConverter.addStep(temp, getMethodName(), vertexIds);
        return new VariantGraphTraversal<>(this.getGraph(), temp, this.variantConverter);
    }

    @Deprecated
    public GraphTraversal<Vertex, Vertex> addV(final Object... keyValues) {
        final StringBuilder temp = new StringBuilder(this.sourceString.toString());
        this.variantConverter.addStep(temp, getMethodName(), keyValues);
        return new VariantGraphTraversal<>(this.getGraph(), temp, this.variantConverter);
    }

    public GraphTraversal<Vertex, Vertex> addV(final String label) {
        final StringBuilder temp = new StringBuilder(this.sourceString.toString());
        this.variantConverter.addStep(temp, getMethodName(), label);
        return new VariantGraphTraversal<>(this.getGraph(), temp, this.variantConverter);
    }

    public GraphTraversal<Vertex, Vertex> addV() {
        final StringBuilder temp = new StringBuilder(this.sourceString.toString());
        this.variantConverter.addStep(temp, getMethodName());
        return new VariantGraphTraversal<>(this.getGraph(), temp, this.variantConverter);
    }

    public <S> GraphTraversal<S, S> inject(S... starts) {
        final StringBuilder temp = new StringBuilder(this.sourceString.toString());
        this.variantConverter.addStep(temp, getMethodName(), starts);
        return new VariantGraphTraversal<>(this.getGraph(), temp, this.variantConverter);
    }

    ///

    @Override
    public GraphTraversalSource withComputer(final Computer computer) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), computer);
        return this;
    }

    @Override
    public GraphTraversalSource withComputer(final Class<? extends GraphComputer> graphComputerClass) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), graphComputerClass);
        return this;
    }

    @Override
    public GraphTraversalSource withComputer() {
        this.variantConverter.addStep(this.sourceString, getMethodName());
        return this;
    }

    @Override
    public GraphTraversalSource withStrategies(final TraversalStrategy... traversalStrategies) {
        return super.withStrategies(traversalStrategies);
        //this.variantConverter.addStep(this.sourceString, getMethodName(), traversalStrategies);
        //return this;
    }

    @Override
    @SuppressWarnings({"unchecked", "varargs"})
    public GraphTraversalSource withoutStrategies(final Class<? extends TraversalStrategy>... traversalStrategyClasses) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), traversalStrategyClasses);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), splitOperator, mergeOperator);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator, final BinaryOperator<A> mergeOperator) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), initialValue, splitOperator, mergeOperator);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), initialValue);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), initialValue);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final UnaryOperator<A> splitOperator) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), initialValue, splitOperator);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final UnaryOperator<A> splitOperator) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), initialValue, splitOperator);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final Supplier<A> initialValue, final BinaryOperator<A> mergeOperator) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), initialValue, mergeOperator);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSack(final A initialValue, final BinaryOperator<A> mergeOperator) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), initialValue, mergeOperator);
        return this;
    }

    /////

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue, final BinaryOperator<A> reducer) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), key, initialValue, reducer);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final A initialValue, final BinaryOperator<A> reducer) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), key, initialValue, reducer);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final A initialValue) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), key, initialValue);
        return this;
    }

    @Override
    public <A> GraphTraversalSource withSideEffect(final String key, final Supplier<A> initialValue) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), key, initialValue);
        return this;
    }

    /////

    @Override
    public GraphTraversalSource withBulk(final boolean useBulk) {
        this.variantConverter.addStep(this.sourceString, getMethodName(), useBulk);
        return this;
    }

    @Override
    public GraphTraversalSource withPath() {
        this.variantConverter.addStep(this.sourceString, getMethodName());
        return this;
    }

}
