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

import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VariantGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {


    public StringBuilder variantString;
    protected VariantConverter variantConverter;

    public VariantGraphTraversal(final Graph graph, final StringBuilder variantString, final VariantConverter variantConverter) {
        super(graph);
        this.variantConverter = variantConverter;
        this.variantString = variantString;
        __.EMPTY_GRAPH_TRAVERSAL = () -> {
            final StringBuilder builder = new StringBuilder("__");
            return new VariantGraphTraversal<>(EmptyGraph.instance(), builder, this.variantConverter);
        };
    }

    public String toString() {
        return this.variantString.toString();
    }

    @Override
    public void applyStrategies() {
        if (!(this.getParent() instanceof EmptyStep)) {
            return;
        }
        try {
            final String jythonString = this.variantConverter.compileVariant(this.variantString);
            __.EMPTY_GRAPH_TRAVERSAL = DefaultGraphTraversal::new;
            ScriptEngine groovy = ScriptEngineCache.get("gremlin-groovy");
            final Bindings groovyBindings = new SimpleBindings();
            groovyBindings.put("g", new GraphTraversalSource(this.getGraph().get(), this.getStrategies()));
            Traversal.Admin<S, E> traversal = (Traversal.Admin<S, E>) groovy.eval(jythonString, groovyBindings);
            assert !traversal.isLocked();
            this.sideEffects = traversal.getSideEffects();
            this.strategies = traversal.getStrategies();
            traversal.getSteps().forEach(step -> this.addStep(step));
            super.applyStrategies();
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private static String getMethodName() {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    //////////////////////////

    public <E2> GraphTraversal<S, E2> map(final Function<Traverser<E>, E2> function) {
        this.variantConverter.step(this.variantString, getMethodName(), function);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> map(final Traversal<?, E2> mapTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), mapTraversal);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> flatMap(final Function<Traverser<E>, Iterator<E2>> function) {
        this.variantConverter.step(this.variantString, getMethodName(), function);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> flatMap(final Traversal<?, E2> flatMapTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), flatMapTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Object> id() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, String> label() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> identity() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> constant(final E2 e) {
        this.variantConverter.step(this.variantString, getMethodName(), e);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> V(final Object... vertexIdsOrElements) {
        this.variantConverter.step(this.variantString, getMethodName(), vertexIdsOrElements);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> to(final Direction direction, final String... edgeLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), direction, edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> out(final String... edgeLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> in(final String... edgeLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> both(final String... edgeLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> toE(final Direction direction, final String... edgeLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), direction, edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> outE(final String... edgeLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> inE(final String... edgeLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> bothE(final String... edgeLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> toV(final Direction direction) {
        this.variantConverter.step(this.variantString, getMethodName(), direction);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> inV() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> outV() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> bothV() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> otherV() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> order() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> order(final Scope scope) {
        this.variantConverter.step(this.variantString, getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, ? extends Property<E2>> properties(final String... propertyKeys) {
        this.variantConverter.step(this.variantString, getMethodName(), propertyKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> values(final String... propertyKeys) {
        this.variantConverter.step(this.variantString, getMethodName(), propertyKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> propertyMap(final String... propertyKeys) {
        this.variantConverter.step(this.variantString, getMethodName(), propertyKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> valueMap(final String... propertyKeys) {
        this.variantConverter.step(this.variantString, getMethodName(), propertyKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        this.variantConverter.step(this.variantString, getMethodName(), includeTokens, propertyKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Collection<E2>> select(final Column column) {
        this.variantConverter.step(this.variantString, getMethodName(), column);
        return (GraphTraversal) this;
    }

    @Deprecated
    public <E2> GraphTraversal<S, E2> mapValues() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    @Deprecated
    public <E2> GraphTraversal<S, E2> mapKeys() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, String> key() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> value() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Path> path() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> match(final Traversal<?, ?>... matchTraversals) {
        this.variantConverter.step(this.variantString, getMethodName(), matchTraversals);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> sack() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Integer> loops() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> project(final String projectKey, final String... otherProjectKeys) {
        this.variantConverter.step(this.variantString, getMethodName(), projectKey, otherProjectKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> select(final Pop pop, final String selectKey1, final String selectKey2, String... otherSelectKeys) {
        this.variantConverter.step(this.variantString, getMethodName(), pop, selectKey1, selectKey2, otherSelectKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> select(final String selectKey1, final String selectKey2, String... otherSelectKeys) {
        this.variantConverter.step(this.variantString, getMethodName(), selectKey1, selectKey2, otherSelectKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> select(final Pop pop, final String selectKey) {
        this.variantConverter.step(this.variantString, getMethodName(), pop, selectKey);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> select(final String selectKey) {
        this.variantConverter.step(this.variantString, getMethodName(), selectKey);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> unfold() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, List<E>> fold() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> fold(final E2 seed, final BiFunction<E2, E, E2> foldFunction) {
        this.variantConverter.step(this.variantString, getMethodName(), seed, foldFunction);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Long> count() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Long> count(final Scope scope) {
        this.variantConverter.step(this.variantString, getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> sum() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> sum(final Scope scope) {
        this.variantConverter.step(this.variantString, getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> max() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> max(final Scope scope) {
        this.variantConverter.step(this.variantString, getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> min() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> min(final Scope scope) {
        this.variantConverter.step(this.variantString, getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> mean() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> mean(final Scope scope) {
        this.variantConverter.step(this.variantString, getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <K, V> GraphTraversal<S, Map<K, V>> group() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    @Deprecated
    public <K, V> GraphTraversal<S, Map<K, V>> groupV3d0() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public <K> GraphTraversal<S, Map<K, Long>> groupCount() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Tree> tree() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> addV(final String vertexLabel) {
        this.variantConverter.step(this.variantString, getMethodName(), vertexLabel);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> addV() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    @Deprecated
    public GraphTraversal<S, Vertex> addV(final Object... propertyKeyValues) {
        this.variantConverter.step(this.variantString, getMethodName(), propertyKeyValues);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> addE(final String edgeLabel) {
        this.variantConverter.step(this.variantString, getMethodName(), edgeLabel);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> to(final String toStepLabel) {
        this.variantConverter.step(this.variantString, getMethodName(), toStepLabel);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> from(final String fromStepLabel) {
        this.variantConverter.step(this.variantString, getMethodName(), fromStepLabel);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> to(final Traversal<E, Vertex> toVertex) {
        this.variantConverter.step(this.variantString, getMethodName(), toVertex);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> from(final Traversal<E, Vertex> fromVertex) {
        this.variantConverter.step(this.variantString, getMethodName(), fromVertex);
        return (GraphTraversal) this;
    }

    @Deprecated
    public GraphTraversal<S, Edge> addE(final Direction direction, final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        this.variantConverter.step(this.variantString, getMethodName(), direction, firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
        return (GraphTraversal) this;
    }

    @Deprecated
    public GraphTraversal<S, Edge> addOutE(final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        this.variantConverter.step(this.variantString, getMethodName(), firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
        return (GraphTraversal) this;
    }

    @Deprecated
    public GraphTraversal<S, Edge> addInE(final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        this.variantConverter.step(this.variantString, getMethodName(), firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
        return (GraphTraversal) this;
    }

    ///////////////////// FILTER STEPS /////////////////////

    public GraphTraversal<S, E> filter(final Predicate<Traverser<E>> predicate) {
        this.variantConverter.step(this.variantString, getMethodName(), predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> filter(final Traversal<?, ?> filterTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), filterTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> or(final Traversal<?, ?>... orTraversals) {
        this.variantConverter.step(this.variantString, getMethodName(), orTraversals);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> and(final Traversal<?, ?>... andTraversals) {
        this.variantConverter.step(this.variantString, getMethodName(), andTraversals);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> inject(final E... injections) {
        this.variantConverter.step(this.variantString, getMethodName(), injections);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> dedup(final Scope scope, final String... dedupLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), scope, dedupLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> dedup(final String... dedupLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), dedupLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> where(final String startKey, final P<String> predicate) {
        this.variantConverter.step(this.variantString, getMethodName(), startKey, predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> where(final P<String> predicate) {
        this.variantConverter.step(this.variantString, getMethodName(), predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> where(final Traversal<?, ?> whereTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), whereTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String propertyKey, final P<?> predicate) {
        this.variantConverter.step(this.variantString, getMethodName(), propertyKey, predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final T accessor, final P<?> predicate) {
        this.variantConverter.step(this.variantString, getMethodName(), accessor, predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String propertyKey, final Object value) {
        this.variantConverter.step(this.variantString, getMethodName(), propertyKey, value);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final T accessor, final Object value) {
        this.variantConverter.step(this.variantString, getMethodName(), accessor, value);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String label, final String propertyKey, final P<?> predicate) {
        this.variantConverter.step(this.variantString, getMethodName(), label, propertyKey, predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String label, final String propertyKey, final Object value) {
        this.variantConverter.step(this.variantString, getMethodName(), label, propertyKey, value);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final T accessor, final Traversal<?, ?> propertyTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), accessor, propertyTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String propertyKey, final Traversal<?, ?> propertyTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), propertyKey, propertyTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String propertyKey) {
        this.variantConverter.step(this.variantString, getMethodName(), propertyKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> hasNot(final String propertyKey) {
        this.variantConverter.step(this.variantString, getMethodName(), propertyKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> hasLabel(final String... labels) {
        this.variantConverter.step(this.variantString, getMethodName(), labels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> hasId(final Object... ids) {
        this.variantConverter.step(this.variantString, getMethodName(), ids);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> hasKey(final String... keys) {
        this.variantConverter.step(this.variantString, getMethodName(), keys);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> hasValue(final Object... values) {
        this.variantConverter.step(this.variantString, getMethodName(), values);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> is(final P<E> predicate) {
        this.variantConverter.step(this.variantString, getMethodName(), predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> is(final Object value) {
        this.variantConverter.step(this.variantString, getMethodName(), value);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> not(final Traversal<?, ?> notTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), notTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> coin(final double probability) {
        this.variantConverter.step(this.variantString, getMethodName(), probability);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> range(final long low, final long high) {
        this.variantConverter.step(this.variantString, getMethodName(), low, high);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> range(final Scope scope, final long low, final long high) {
        this.variantConverter.step(this.variantString, getMethodName(), scope, low, high);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> limit(final long limit) {
        this.variantConverter.step(this.variantString, getMethodName(), limit);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> limit(final Scope scope, final long limit) {
        this.variantConverter.step(this.variantString, getMethodName(), scope, limit);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> tail() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> tail(final long limit) {
        this.variantConverter.step(this.variantString, getMethodName(), limit);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> tail(final Scope scope) {
        this.variantConverter.step(this.variantString, getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> tail(final Scope scope, final long limit) {
        this.variantConverter.step(this.variantString, getMethodName(), scope, limit);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> timeLimit(final long timeLimit) {
        this.variantConverter.step(this.variantString, getMethodName(), timeLimit);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> simplePath() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> cyclicPath() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> sample(final int amountToSample) {
        this.variantConverter.step(this.variantString, getMethodName(), amountToSample);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> sample(final Scope scope, final int amountToSample) {
        this.variantConverter.step(this.variantString, getMethodName(), scope, amountToSample);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> drop() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public GraphTraversal<S, E> sideEffect(final Consumer<Traverser<E>> consumer) {
        this.variantConverter.step(this.variantString, getMethodName(), consumer);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> sideEffect(final Traversal<?, ?> sideEffectTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), sideEffectTraversal);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> cap(final String sideEffectKey, final String... sideEffectKeys) {
        this.variantConverter.step(this.variantString, getMethodName(), sideEffectKey, sideEffectKeys);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> subgraph(final String sideEffectKey) {
        this.variantConverter.step(this.variantString, getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> aggregate(final String sideEffectKey) {
        this.variantConverter.step(this.variantString, getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> group(final String sideEffectKey) {
        this.variantConverter.step(this.variantString, getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> groupV3d0(final String sideEffectKey) {
        this.variantConverter.step(this.variantString, getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> groupCount(final String sideEffectKey) {
        this.variantConverter.step(this.variantString, getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> tree(final String sideEffectKey) {
        this.variantConverter.step(this.variantString, getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public <V, U> GraphTraversal<S, E> sack(final BiFunction<V, U, V> sackOperator) {
        this.variantConverter.step(this.variantString, getMethodName(), sackOperator);
        return (GraphTraversal) this;
    }


    @Deprecated
    public <V, U> GraphTraversal<S, E> sack(final BiFunction<V, U, V> sackOperator, final String elementPropertyKey) {
        this.variantConverter.step(this.variantString, getMethodName(), sackOperator, elementPropertyKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> store(final String sideEffectKey) {
        this.variantConverter.step(this.variantString, getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> profile(final String sideEffectKey) {
        this.variantConverter.step(this.variantString, getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> property(final VertexProperty.Cardinality cardinality, final Object key, final Object value, final Object... keyValues) {
        this.variantConverter.step(this.variantString, getMethodName(), cardinality, key, value, keyValues);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> property(final Object key, final Object value, final Object... keyValues) {
        this.variantConverter.step(this.variantString, getMethodName(), key, value, keyValues);
        return (GraphTraversal) this;
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public <M, E2> GraphTraversal<S, E2> branch(final Traversal<?, M> branchTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), branchTraversal);
        return (GraphTraversal) this;
    }

    public <M, E2> GraphTraversal<S, E2> branch(final Function<Traverser<E>, M> function) {
        this.variantConverter.step(this.variantString, getMethodName(), function);
        return (GraphTraversal) this;
    }

    public <M, E2> GraphTraversal<S, E2> choose(final Traversal<?, M> choiceTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), choiceTraversal);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> choose(final Traversal<?, ?> traversalPredicate,
                                             final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        this.variantConverter.step(this.variantString, getMethodName(), traversalPredicate, trueChoice, falseChoice);
        return (GraphTraversal) this;
    }

    public <M, E2> GraphTraversal<S, E2> choose(final Function<E, M> choiceFunction) {
        this.variantConverter.step(this.variantString, getMethodName(), choiceFunction);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> choose(final Predicate<E> choosePredicate,
                                             final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        this.variantConverter.step(this.variantString, getMethodName(), choosePredicate, trueChoice, falseChoice);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> optional(final Traversal<?, E2> optionalTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), optionalTraversal);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> union(final Traversal<?, E2>... unionTraversals) {
        this.variantConverter.step(this.variantString, getMethodName(), unionTraversals);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> coalesce(final Traversal<?, E2>... coalesceTraversals) {
        this.variantConverter.step(this.variantString, getMethodName(), coalesceTraversals);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> repeat(final Traversal<?, E> repeatTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), repeatTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> emit(final Traversal<?, ?> emitTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), emitTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> emit(final Predicate<Traverser<E>> emitPredicate) {
        this.variantConverter.step(this.variantString, getMethodName(), emitPredicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> emit() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> until(final Traversal<?, ?> untilTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), untilTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> until(final Predicate<Traverser<E>> untilPredicate) {
        this.variantConverter.step(this.variantString, getMethodName(), untilPredicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> times(final int maxLoops) {
        this.variantConverter.step(this.variantString, getMethodName(), maxLoops);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> local(final Traversal<?, E2> localTraversal) {
        this.variantConverter.step(this.variantString, getMethodName(), localTraversal);
        return (GraphTraversal) this;
    }

    /////////////////// VERTEX PROGRAM STEPS ////////////////

    public GraphTraversal<S, E> pageRank() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> pageRank(final double alpha) {
        this.variantConverter.step(this.variantString, getMethodName(), alpha);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> peerPressure() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> program(final VertexProgram<?> vertexProgram) {
        this.variantConverter.step(this.variantString, getMethodName(), vertexProgram);
        return (GraphTraversal) this;
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public GraphTraversal<S, E> as(final String stepLabel, final String... stepLabels) {
        this.variantConverter.step(this.variantString, getMethodName(), stepLabel, stepLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> barrier() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> barrier(final int maxBarrierSize) {
        this.variantConverter.step(this.variantString, getMethodName(), maxBarrierSize);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> barrier(final Consumer<TraverserSet<Object>> barrierConsumer) {
        this.variantConverter.step(this.variantString, getMethodName(), barrierConsumer);
        return (GraphTraversal) this;
    }


    //// BY-MODULATORS

    public GraphTraversal<S, E> by() {
        this.variantConverter.step(this.variantString, getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> by(final Traversal<?, ?> traversal) {
        this.variantConverter.step(this.variantString, getMethodName(), traversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> by(final T token) {
        this.variantConverter.step(this.variantString, getMethodName(), token);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> by(final String key) {
        this.variantConverter.step(this.variantString, getMethodName(), key);
        return (GraphTraversal) this;
    }

    public <V> GraphTraversal<S, E> by(final Function<V, Object> function) {
        this.variantConverter.step(this.variantString, getMethodName(), function);
        return (GraphTraversal) this;
    }

    //// COMPARATOR BY-MODULATORS

    public <V> GraphTraversal<S, E> by(final Traversal<?, ?> traversal, final Comparator<V> comparator) {
        this.variantConverter.step(this.variantString, getMethodName(), traversal, comparator);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> by(final Comparator<E> comparator) {
        this.variantConverter.step(this.variantString, getMethodName(), comparator);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> by(final Order order) {
        this.variantConverter.step(this.variantString, getMethodName(), order);
        return (GraphTraversal) this;
    }

    public <V> GraphTraversal<S, E> by(final String key, final Comparator<V> comparator) {
        this.variantConverter.step(this.variantString, getMethodName(), key, comparator);
        return (GraphTraversal) this;
    }

    public <U> GraphTraversal<S, E> by(final Function<U, Object> function, final Comparator comparator) {
        this.variantConverter.step(this.variantString, getMethodName(), function, comparator);
        return (GraphTraversal) this;
    }

    ////

    public <M, E2> GraphTraversal<S, E> option(final M pickToken, final Traversal<E, E2> traversalOption) {
        this.variantConverter.step(this.variantString, getMethodName(), pickToken, traversalOption);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E> option(final Traversal<E, E2> traversalOption) {
        this.variantConverter.step(this.variantString, getMethodName(), traversalOption);
        return (GraphTraversal) this;
    }
}
