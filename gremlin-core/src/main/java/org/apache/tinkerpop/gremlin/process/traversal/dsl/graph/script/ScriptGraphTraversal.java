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
import org.apache.tinkerpop.gremlin.process.traversal.util.Translator;
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
public class ScriptGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    protected Translator translator;

    public ScriptGraphTraversal(final Graph graph, final Translator translator) {
        super(graph);
        this.translator = translator;
    }

    public String getTraversalScript() {
        return this.translator.getTraversalScript();
    }

    @Override
    public void applyStrategies() {
        if (!(this.getParent() instanceof EmptyStep)) {
            return;
        }
        try {
            final String traversalScriptString = this.getTraversalScript();
            __.setAnonymousTraversalSupplier(null);
            ScriptEngine engine = ScriptEngineCache.get(this.translator.getScriptEngine());
            final Bindings bindings = new SimpleBindings();
            bindings.put(this.translator.getAlias(), new GraphTraversalSource(this.getGraph().get(), this.getStrategies()));
            Traversal.Admin<S, E> traversal = (Traversal.Admin<S, E>) engine.eval(traversalScriptString, bindings);
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
        this.translator.addStep(getMethodName(), function);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> map(final Traversal<?, E2> mapTraversal) {
        this.translator.addStep(getMethodName(), mapTraversal);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> flatMap(final Function<Traverser<E>, Iterator<E2>> function) {
        this.translator.addStep(getMethodName(), function);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> flatMap(final Traversal<?, E2> flatMapTraversal) {
        this.translator.addStep(getMethodName(), flatMapTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Object> id() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, String> label() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> identity() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> constant(final E2 e) {
        this.translator.addStep(getMethodName(), e);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> V(final Object... vertexIdsOrElements) {
        this.translator.addStep(getMethodName(), vertexIdsOrElements);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> to(final Direction direction, final String... edgeLabels) {
        this.translator.addStep(getMethodName(), direction, edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> out(final String... edgeLabels) {
        this.translator.addStep(getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> in(final String... edgeLabels) {
        this.translator.addStep(getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> both(final String... edgeLabels) {
        this.translator.addStep(getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> toE(final Direction direction, final String... edgeLabels) {
        this.translator.addStep(getMethodName(), direction, edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> outE(final String... edgeLabels) {
        this.translator.addStep(getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> inE(final String... edgeLabels) {
        this.translator.addStep(getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> bothE(final String... edgeLabels) {
        this.translator.addStep(getMethodName(), edgeLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> toV(final Direction direction) {
        this.translator.addStep(getMethodName(), direction);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> inV() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> outV() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> bothV() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> otherV() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> order() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> order(final Scope scope) {
        this.translator.addStep(getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, ? extends Property<E2>> properties(final String... propertyKeys) {
        this.translator.addStep(getMethodName(), propertyKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> values(final String... propertyKeys) {
        this.translator.addStep(getMethodName(), propertyKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> propertyMap(final String... propertyKeys) {
        this.translator.addStep(getMethodName(), propertyKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> valueMap(final String... propertyKeys) {
        this.translator.addStep(getMethodName(), propertyKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        this.translator.addStep(getMethodName(), includeTokens, propertyKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Collection<E2>> select(final Column column) {
        this.translator.addStep(getMethodName(), column);
        return (GraphTraversal) this;
    }

    @Deprecated
    public <E2> GraphTraversal<S, E2> mapValues() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    @Deprecated
    public <E2> GraphTraversal<S, E2> mapKeys() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, String> key() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> value() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Path> path() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> match(final Traversal<?, ?>... matchTraversals) {
        this.translator.addStep(getMethodName(), matchTraversals);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> sack() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Integer> loops() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> project(final String projectKey, final String... otherProjectKeys) {
        this.translator.addStep(getMethodName(), projectKey, otherProjectKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> select(final Pop pop, final String selectKey1, final String selectKey2, String... otherSelectKeys) {
        this.translator.addStep(getMethodName(), pop, selectKey1, selectKey2, otherSelectKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, Map<String, E2>> select(final String selectKey1, final String selectKey2, String... otherSelectKeys) {
        this.translator.addStep(getMethodName(), selectKey1, selectKey2, otherSelectKeys);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> select(final Pop pop, final String selectKey) {
        this.translator.addStep(getMethodName(), pop, selectKey);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> select(final String selectKey) {
        this.translator.addStep(getMethodName(), selectKey);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> unfold() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, List<E>> fold() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> fold(final E2 seed, final BiFunction<E2, E, E2> foldFunction) {
        this.translator.addStep(getMethodName(), seed, foldFunction);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Long> count() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Long> count(final Scope scope) {
        this.translator.addStep(getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> sum() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> sum(final Scope scope) {
        this.translator.addStep(getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> max() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> max(final Scope scope) {
        this.translator.addStep(getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> min() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> min(final Scope scope) {
        this.translator.addStep(getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> mean() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public <E2 extends Number> GraphTraversal<S, E2> mean(final Scope scope) {
        this.translator.addStep(getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <K, V> GraphTraversal<S, Map<K, V>> group() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    @Deprecated
    public <K, V> GraphTraversal<S, Map<K, V>> groupV3d0() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public <K> GraphTraversal<S, Map<K, Long>> groupCount() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Tree> tree() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> addV(final String vertexLabel) {
        this.translator.addStep(getMethodName(), vertexLabel);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Vertex> addV() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    @Deprecated
    public GraphTraversal<S, Vertex> addV(final Object... propertyKeyValues) {
        this.translator.addStep(getMethodName(), propertyKeyValues);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> addE(final String edgeLabel) {
        this.translator.addStep(getMethodName(), edgeLabel);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> to(final String toStepLabel) {
        this.translator.addStep(getMethodName(), toStepLabel);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> from(final String fromStepLabel) {
        this.translator.addStep(getMethodName(), fromStepLabel);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> to(final Traversal<E, Vertex> toVertex) {
        this.translator.addStep(getMethodName(), toVertex);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> from(final Traversal<E, Vertex> fromVertex) {
        this.translator.addStep(getMethodName(), fromVertex);
        return (GraphTraversal) this;
    }

    @Deprecated
    public GraphTraversal<S, Edge> addE(final Direction direction, final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        this.translator.addStep(getMethodName(), direction, firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
        return (GraphTraversal) this;
    }

    @Deprecated
    public GraphTraversal<S, Edge> addOutE(final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        this.translator.addStep(getMethodName(), firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
        return (GraphTraversal) this;
    }

    @Deprecated
    public GraphTraversal<S, Edge> addInE(final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        this.translator.addStep(getMethodName(), firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
        return (GraphTraversal) this;
    }

    ///////////////////// FILTER STEPS /////////////////////

    public GraphTraversal<S, E> filter(final Predicate<Traverser<E>> predicate) {
        this.translator.addStep(getMethodName(), predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> filter(final Traversal<?, ?> filterTraversal) {
        this.translator.addStep(getMethodName(), filterTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> or(final Traversal<?, ?>... orTraversals) {
        this.translator.addStep(getMethodName(), orTraversals);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> and(final Traversal<?, ?>... andTraversals) {
        this.translator.addStep(getMethodName(), andTraversals);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> inject(final E... injections) {
        this.translator.addStep(getMethodName(), injections);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> dedup(final Scope scope, final String... dedupLabels) {
        this.translator.addStep(getMethodName(), scope, dedupLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> dedup(final String... dedupLabels) {
        this.translator.addStep(getMethodName(), dedupLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> where(final String startKey, final P<String> predicate) {
        this.translator.addStep(getMethodName(), startKey, predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> where(final P<String> predicate) {
        this.translator.addStep(getMethodName(), predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> where(final Traversal<?, ?> whereTraversal) {
        this.translator.addStep(getMethodName(), whereTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String propertyKey, final P<?> predicate) {
        this.translator.addStep(getMethodName(), propertyKey, predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final T accessor, final P<?> predicate) {
        this.translator.addStep(getMethodName(), accessor, predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String propertyKey, final Object value) {
        this.translator.addStep(getMethodName(), propertyKey, value);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final T accessor, final Object value) {
        this.translator.addStep(getMethodName(), accessor, value);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String label, final String propertyKey, final P<?> predicate) {
        this.translator.addStep(getMethodName(), label, propertyKey, predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String label, final String propertyKey, final Object value) {
        this.translator.addStep(getMethodName(), label, propertyKey, value);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final T accessor, final Traversal<?, ?> propertyTraversal) {
        this.translator.addStep(getMethodName(), accessor, propertyTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String propertyKey, final Traversal<?, ?> propertyTraversal) {
        this.translator.addStep(getMethodName(), propertyKey, propertyTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> has(final String propertyKey) {
        this.translator.addStep(getMethodName(), propertyKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> hasNot(final String propertyKey) {
        this.translator.addStep(getMethodName(), propertyKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> hasLabel(final String... labels) {
        this.translator.addStep(getMethodName(), labels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> hasId(final Object... ids) {
        this.translator.addStep(getMethodName(), ids);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> hasKey(final String... keys) {
        this.translator.addStep(getMethodName(), keys);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> hasValue(final Object... values) {
        this.translator.addStep(getMethodName(), values);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> is(final P<E> predicate) {
        this.translator.addStep(getMethodName(), predicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> is(final Object value) {
        this.translator.addStep(getMethodName(), value);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> not(final Traversal<?, ?> notTraversal) {
        this.translator.addStep(getMethodName(), notTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> coin(final double probability) {
        this.translator.addStep(getMethodName(), probability);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> range(final long low, final long high) {
        this.translator.addStep(getMethodName(), low, high);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> range(final Scope scope, final long low, final long high) {
        this.translator.addStep(getMethodName(), scope, low, high);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> limit(final long limit) {
        this.translator.addStep(getMethodName(), limit);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> limit(final Scope scope, final long limit) {
        this.translator.addStep(getMethodName(), scope, limit);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> tail() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> tail(final long limit) {
        this.translator.addStep(getMethodName(), limit);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> tail(final Scope scope) {
        this.translator.addStep(getMethodName(), scope);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> tail(final Scope scope, final long limit) {
        this.translator.addStep(getMethodName(), scope, limit);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> timeLimit(final long timeLimit) {
        this.translator.addStep(getMethodName(), timeLimit);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> simplePath() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> cyclicPath() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> sample(final int amountToSample) {
        this.translator.addStep(getMethodName(), amountToSample);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> sample(final Scope scope, final int amountToSample) {
        this.translator.addStep(getMethodName(), scope, amountToSample);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> drop() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public GraphTraversal<S, E> sideEffect(final Consumer<Traverser<E>> consumer) {
        this.translator.addStep(getMethodName(), consumer);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> sideEffect(final Traversal<?, ?> sideEffectTraversal) {
        this.translator.addStep(getMethodName(), sideEffectTraversal);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> cap(final String sideEffectKey, final String... sideEffectKeys) {
        this.translator.addStep(getMethodName(), sideEffectKey, sideEffectKeys);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, Edge> subgraph(final String sideEffectKey) {
        this.translator.addStep(getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> aggregate(final String sideEffectKey) {
        this.translator.addStep(getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> group(final String sideEffectKey) {
        this.translator.addStep(getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> groupV3d0(final String sideEffectKey) {
        this.translator.addStep(getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> groupCount(final String sideEffectKey) {
        this.translator.addStep(getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> tree(final String sideEffectKey) {
        this.translator.addStep(getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public <V, U> GraphTraversal<S, E> sack(final BiFunction<V, U, V> sackOperator) {
        this.translator.addStep(getMethodName(), sackOperator);
        return (GraphTraversal) this;
    }


    @Deprecated
    public <V, U> GraphTraversal<S, E> sack(final BiFunction<V, U, V> sackOperator, final String elementPropertyKey) {
        this.translator.addStep(getMethodName(), sackOperator, elementPropertyKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> store(final String sideEffectKey) {
        this.translator.addStep(getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> profile(final String sideEffectKey) {
        this.translator.addStep(getMethodName(), sideEffectKey);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> property(final VertexProperty.Cardinality cardinality, final Object key, final Object value, final Object... keyValues) {
        this.translator.addStep(getMethodName(), cardinality, key, value, keyValues);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> property(final Object key, final Object value, final Object... keyValues) {
        this.translator.addStep(getMethodName(), key, value, keyValues);
        return (GraphTraversal) this;
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public <M, E2> GraphTraversal<S, E2> branch(final Traversal<?, M> branchTraversal) {
        this.translator.addStep(getMethodName(), branchTraversal);
        return (GraphTraversal) this;
    }

    public <M, E2> GraphTraversal<S, E2> branch(final Function<Traverser<E>, M> function) {
        this.translator.addStep(getMethodName(), function);
        return (GraphTraversal) this;
    }

    public <M, E2> GraphTraversal<S, E2> choose(final Traversal<?, M> choiceTraversal) {
        this.translator.addStep(getMethodName(), choiceTraversal);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> choose(final Traversal<?, ?> traversalPredicate,
                                             final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        this.translator.addStep(getMethodName(), traversalPredicate, trueChoice, falseChoice);
        return (GraphTraversal) this;
    }

    public <M, E2> GraphTraversal<S, E2> choose(final Function<E, M> choiceFunction) {
        this.translator.addStep(getMethodName(), choiceFunction);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> choose(final Predicate<E> choosePredicate,
                                             final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        this.translator.addStep(getMethodName(), choosePredicate, trueChoice, falseChoice);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> optional(final Traversal<?, E2> optionalTraversal) {
        this.translator.addStep(getMethodName(), optionalTraversal);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> union(final Traversal<?, E2>... unionTraversals) {
        this.translator.addStep(getMethodName(), unionTraversals);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> coalesce(final Traversal<?, E2>... coalesceTraversals) {
        this.translator.addStep(getMethodName(), coalesceTraversals);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> repeat(final Traversal<?, E> repeatTraversal) {
        this.translator.addStep(getMethodName(), repeatTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> emit(final Traversal<?, ?> emitTraversal) {
        this.translator.addStep(getMethodName(), emitTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> emit(final Predicate<Traverser<E>> emitPredicate) {
        this.translator.addStep(getMethodName(), emitPredicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> emit() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> until(final Traversal<?, ?> untilTraversal) {
        this.translator.addStep(getMethodName(), untilTraversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> until(final Predicate<Traverser<E>> untilPredicate) {
        this.translator.addStep(getMethodName(), untilPredicate);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> times(final int maxLoops) {
        this.translator.addStep(getMethodName(), maxLoops);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E2> local(final Traversal<?, E2> localTraversal) {
        this.translator.addStep(getMethodName(), localTraversal);
        return (GraphTraversal) this;
    }

    /////////////////// VERTEX PROGRAM STEPS ////////////////

    public GraphTraversal<S, E> pageRank() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> pageRank(final double alpha) {
        this.translator.addStep(getMethodName(), alpha);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> peerPressure() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> program(final VertexProgram<?> vertexProgram) {
        this.translator.addStep(getMethodName(), vertexProgram);
        return (GraphTraversal) this;
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public GraphTraversal<S, E> as(final String stepLabel, final String... stepLabels) {
        this.translator.addStep(getMethodName(), stepLabel, stepLabels);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> barrier() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> barrier(final int maxBarrierSize) {
        this.translator.addStep(getMethodName(), maxBarrierSize);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> barrier(final Consumer<TraverserSet<Object>> barrierConsumer) {
        this.translator.addStep(getMethodName(), barrierConsumer);
        return (GraphTraversal) this;
    }


    //// BY-MODULATORS

    public GraphTraversal<S, E> by() {
        this.translator.addStep(getMethodName());
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> by(final Traversal<?, ?> traversal) {
        this.translator.addStep(getMethodName(), traversal);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> by(final T token) {
        this.translator.addStep(getMethodName(), token);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> by(final String key) {
        this.translator.addStep(getMethodName(), key);
        return (GraphTraversal) this;
    }

    public <V> GraphTraversal<S, E> by(final Function<V, Object> function) {
        this.translator.addStep(getMethodName(), function);
        return (GraphTraversal) this;
    }

    //// COMPARATOR BY-MODULATORS

    public <V> GraphTraversal<S, E> by(final Traversal<?, ?> traversal, final Comparator<V> comparator) {
        this.translator.addStep(getMethodName(), traversal, comparator);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> by(final Comparator<E> comparator) {
        this.translator.addStep(getMethodName(), comparator);
        return (GraphTraversal) this;
    }

    public GraphTraversal<S, E> by(final Order order) {
        this.translator.addStep(getMethodName(), order);
        return (GraphTraversal) this;
    }

    public <V> GraphTraversal<S, E> by(final String key, final Comparator<V> comparator) {
        this.translator.addStep(getMethodName(), key, comparator);
        return (GraphTraversal) this;
    }

    public <U> GraphTraversal<S, E> by(final Function<U, Object> function, final Comparator comparator) {
        this.translator.addStep(getMethodName(), function, comparator);
        return (GraphTraversal) this;
    }

    ////

    public <M, E2> GraphTraversal<S, E> option(final M pickToken, final Traversal<E, E2> traversalOption) {
        this.translator.addStep(getMethodName(), pickToken, traversalOption);
        return (GraphTraversal) this;
    }

    public <E2> GraphTraversal<S, E> option(final Traversal<E, E2> traversalOption) {
        this.translator.addStep(getMethodName(), traversalOption);
        return (GraphTraversal) this;
    }
}
