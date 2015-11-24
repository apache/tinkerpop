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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collection;
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
public class __ {

    protected __() {
    }

    //////////////////////////////////////////////////////////////////////

    public static <A> GraphTraversal<A, A> start() {
        return new DefaultGraphTraversal<>();
    }

    public static <A> GraphTraversal<A, A> __(final A... starts) {
        return inject(starts);
    }

    ///////////////////// MAP STEPS /////////////////////

    /**
     * @see {@link GraphTraversal#map(Function)}.
     */
    public static <A, B> GraphTraversal<A, B> map(final Function<Traverser<A>, B> function) {
        return __.<A>start().map(function);
    }

    public static <A, B> GraphTraversal<A, B> map(final Traversal<?, B> mapTraversal) {
        return __.<A>start().map(mapTraversal);
    }

    /**
     * @see {@link GraphTraversal#flatMap(Function)}.
     */
    public static <A, B> GraphTraversal<A, B> flatMap(final Function<Traverser<A>, Iterator<B>> function) {
        return __.<A>start().flatMap(function);
    }

    public static <A, B> GraphTraversal<A, B> flatMap(final Traversal<?, B> flatMapTraversal) {
        return __.<A>start().flatMap(flatMapTraversal);
    }

    /**
     * @see {@link GraphTraversal#identity()}
     */
    public static <A> GraphTraversal<A, A> identity() {
        return __.<A>start().identity();
    }

    /**
     * @see {@link GraphTraversal#constant(Object)}
     */
    public static <A> GraphTraversal<A, A> constant(final A a) {
        return __.<A>start().constant(a);
    }

    /**
     * @see {@link GraphTraversal#label()}
     */
    public static <A> GraphTraversal<A, String> label() {
        return __.<A>start().label();
    }

    /**
     * @see {@link GraphTraversal#id()}
     */
    public static <A> GraphTraversal<A, Object> id() {
        return __.<A>start().id();
    }

    /**
     * @see {@link GraphTraversal#V(Object...)}
     */
    public static <A> GraphTraversal<A, Vertex> V(final Object... vertexIdsOrElements) {
        return __.<A>start().V(vertexIdsOrElements);
    }

    /**
     * @see {@link GraphTraversal#to(Direction, String...)}
     */
    public static <A> GraphTraversal<A, Vertex> to(final Direction direction, final String... edgeLabels) {
        return __.<A>start().to(direction, edgeLabels);
    }

    /**
     * @see {@link GraphTraversal#out(String...)}
     */
    public static <A> GraphTraversal<A, Vertex> out(final String... edgeLabels) {
        return __.<A>start().out(edgeLabels);
    }

    /**
     * @see {@link GraphTraversal#in(String...)}
     */
    public static <A> GraphTraversal<A, Vertex> in(final String... edgeLabels) {
        return __.<A>start().in(edgeLabels);
    }

    /**
     * @see {@link GraphTraversal#both(String...)}
     */
    public static <A> GraphTraversal<A, Vertex> both(final String... edgeLabels) {
        return __.<A>start().both(edgeLabels);
    }

    /**
     * @see {@link GraphTraversal#toE(Direction, String...)}
     */
    public static <A> GraphTraversal<A, Edge> toE(final Direction direction, final String... edgeLabels) {
        return __.<A>start().toE(direction, edgeLabels);
    }

    /**
     * @see {@link GraphTraversal#outE(String...)}
     */
    public static <A> GraphTraversal<A, Edge> outE(final String... edgeLabels) {
        return __.<A>start().outE(edgeLabels);
    }

    /**
     * @see {@link GraphTraversal#inE(String...)}
     */
    public static <A> GraphTraversal<A, Edge> inE(final String... edgeLabels) {
        return __.<A>start().inE(edgeLabels);
    }

    /**
     * @see {@link GraphTraversal#bothE(String...)}
     */
    public static <A> GraphTraversal<A, Edge> bothE(final String... edgeLabels) {
        return __.<A>start().bothE(edgeLabels);
    }

    /**
     * @see {@link GraphTraversal#toV(Direction)}
     */
    public static <A> GraphTraversal<A, Vertex> toV(final Direction direction) {
        return __.<A>start().toV(direction);
    }

    /**
     * @see {@link GraphTraversal#inV()}
     */
    public static <A> GraphTraversal<A, Vertex> inV() {
        return __.<A>start().inV();
    }

    /**
     * @see {@link GraphTraversal#outV()}
     */
    public static <A> GraphTraversal<A, Vertex> outV() {
        return __.<A>start().outV();
    }

    /**
     * @see {@link GraphTraversal#bothV()}
     */
    public static <A> GraphTraversal<A, Vertex> bothV() {
        return __.<A>start().bothV();
    }

    /**
     * @see {@link GraphTraversal#otherV()}
     */
    public static <A> GraphTraversal<A, Vertex> otherV() {
        return __.<A>start().otherV();
    }

    /**
     * @see {@link GraphTraversal#order()}
     */
    public static <A> GraphTraversal<A, A> order() {
        return __.<A>start().order();
    }

    /**
     * @see {@link GraphTraversal#order(Scope)}
     */
    public static <A> GraphTraversal<A, A> order(final Scope scope) {
        return __.<A>start().order(scope);
    }

    /**
     * @see {@link GraphTraversal#properties(String...)}
     */
    public static <A, B> GraphTraversal<A, ? extends Property<B>> properties(final String... propertyKeys) {
        return __.<A>start().properties(propertyKeys);
    }

    /**
     * @see {@link GraphTraversal#values(String...)}
     */
    public static <A, B> GraphTraversal<A, B> values(final String... propertyKeys) {
        return __.<A>start().values(propertyKeys);
    }

    /**
     * @see {@link GraphTraversal#propertyMap(String...)}
     */
    public static <A, B> GraphTraversal<A, Map<String, B>> propertyMap(final String... propertyKeys) {
        return __.<A>start().propertyMap(propertyKeys);
    }

    /**
     * @see {@link GraphTraversal#valueMap(String...)}
     */
    public static <A, B> GraphTraversal<A, Map<String, B>> valueMap(final String... propertyKeys) {
        return __.<A>start().valueMap(propertyKeys);
    }

    /**
     * @see {@link GraphTraversal#valueMap(boolean, String...)}
     */
    public static <A, B> GraphTraversal<A, Map<String, B>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return __.<A>start().valueMap(includeTokens, propertyKeys);
    }

    public static <A, B> GraphTraversal<A, Collection<B>> select(final Column column) {
        return __.<A>start().select(column);
    }

    /**
     * @see {@link GraphTraversal#mapValues()}
     */
    @Deprecated
    public static <A, B> GraphTraversal<A, B> mapValues() {
        return __.<A>start().mapValues();
    }

    /**
     * @see {@link GraphTraversal#mapKeys()}
     */
    @Deprecated
    public static <A, B> GraphTraversal<A, B> mapKeys() {
        return __.<A>start().mapKeys();
    }

    /**
     * @see {@link GraphTraversal#key()}
     */
    public static <A> GraphTraversal<A, String> key() {
        return __.<A>start().key();
    }

    /**
     * @see {@link GraphTraversal#value()}
     */
    public static <A, B> GraphTraversal<A, B> value() {
        return __.<A>start().value();
    }

    /**
     * @see {@link GraphTraversal#path()}
     */
    public static <A> GraphTraversal<A, Path> path() {
        return __.<A>start().path();
    }

    /**
     * @see {@link GraphTraversal#match(Traversal[])}
     */
    public static <A, B> GraphTraversal<A, Map<String, B>> match(final Traversal<?, ?>... matchTraversals) {
        return __.<A>start().match(matchTraversals);
    }

    /**
     * @see {@link GraphTraversal#sack()}
     */
    public static <A, B> GraphTraversal<A, B> sack() {
        return __.<A>start().sack();
    }

    /**
     * @see {@link GraphTraversal#loops()}
     */
    public static <A> GraphTraversal<A, Integer> loops() {
        return __.<A>start().loops();
    }

    /**
     * @see {@link GraphTraversal#select(Pop, String)}
     */
    public static <A, B> GraphTraversal<A, B> select(final Pop pop, final String selectKey) {
        return __.<A>start().select(pop, selectKey);
    }

    /**
     * @see {@link GraphTraversal#select(String)}
     */
    public static <A, B> GraphTraversal<A, B> select(final String selectKey) {
        return __.<A>start().select(selectKey);
    }

    /**
     * @see {@link GraphTraversal#select(Pop, String, String, String...)}
     */
    public static <A, B> GraphTraversal<A, Map<String, B>> select(final Pop pop, final String selectKey1, final String selectKey2, final String... otherSelectKeys) {
        return __.<A>start().select(pop, selectKey1, selectKey2, otherSelectKeys);
    }

    /**
     * @see {@link GraphTraversal#select(String, String, String...)}
     */
    public static <A, B> GraphTraversal<A, Map<String, B>> select(final String selectKey1, final String selectKey2, final String... otherSelectKeys) {
        return __.<A>start().select(selectKey1, selectKey2, otherSelectKeys);
    }

    /**
     * @see {@link GraphTraversal#unfold()}
     */
    public static <A> GraphTraversal<A, A> unfold() {
        return __.<A>start().unfold();
    }

    /**
     * @see {@link GraphTraversal#fold()}
     */
    public static <A> GraphTraversal<A, List<A>> fold() {
        return __.<A>start().fold();
    }

    /**
     * @see {@link GraphTraversal#fold(Object, BiFunction)}
     */
    public static <A, B> GraphTraversal<A, B> fold(final B seed, final BiFunction<B, A, B> foldFunction) {
        return __.<A>start().fold(seed, foldFunction);
    }

    /**
     * @see {@link GraphTraversal#count()}
     */
    public static <A> GraphTraversal<A, Long> count() {
        return __.<A>start().count();
    }

    /**
     * @see {@link GraphTraversal#count(Scope)}
     */
    public static <A> GraphTraversal<A, Long> count(final Scope scope) {
        return __.<A>start().count(scope);
    }

    /**
     * @see {@link GraphTraversal#sum()}
     */
    public static <A> GraphTraversal<A, Double> sum() {
        return __.<A>start().sum();
    }

    /**
     * @see {@link GraphTraversal#sum(Scope)}
     */
    public static <A> GraphTraversal<A, Double> sum(final Scope scope) {
        return __.<A>start().sum(scope);
    }

    /**
     * @see {@link GraphTraversal#min()}
     */
    public static <A, B extends Number> GraphTraversal<A, B> min() {
        return __.<A>start().min();
    }

    /**
     * @see {@link GraphTraversal#min(Scope)}
     */
    public static <A, B extends Number> GraphTraversal<A, B> min(final Scope scope) {
        return __.<A>start().min(scope);
    }

    /**
     * @see {@link GraphTraversal#max()}
     */
    public static <A, B extends Number> GraphTraversal<A, B> max() {
        return __.<A>start().max();
    }

    /**
     * @see {@link GraphTraversal#max(Scope)}
     */
    public static <A, B extends Number> GraphTraversal<A, B> max(final Scope scope) {
        return __.<A>start().max(scope);
    }

    /**
     * @see {@link GraphTraversal#mean()}
     */
    public static <A> GraphTraversal<A, Double> mean() {
        return __.<A>start().mean();
    }

    /**
     * @see {@link GraphTraversal#mean(Scope)}
     */
    public static <A> GraphTraversal<A, Double> mean(final Scope scope) {
        return __.<A>start().mean(scope);
    }

    /**
     * @see {@link GraphTraversal#group()}
     */
    public static <A, K, V> GraphTraversal<A, Map<K, V>> group() {
        return __.<A>start().group();
    }

    /**
     * @see {@link GraphTraversal#group()}
     */
    @Deprecated
    public static <A, K, V> GraphTraversal<A, Map<K, V>> groupV3d0() {
        return __.<A>start().groupV3d0();
    }

    /**
     * @see {@link GraphTraversal#groupCount()}
     */
    public static <A, K> GraphTraversal<A, Map<K, Long>> groupCount() {
        return __.<A>start().<K>groupCount();
    }

    /**
     * @see {@link GraphTraversal#tree()}
     */
    public static <A> GraphTraversal<A, Tree> tree() {
        return __.<A>start().tree();
    }

    /**
     * @see {@link GraphTraversal#addV(String)}
     */
    public static <A> GraphTraversal<A, Vertex> addV(final String vertexLabel) {
        return __.<A>start().addV(vertexLabel);
    }

    /**
     * @see {@link GraphTraversal#addV()}
     */
    public static <A> GraphTraversal<A, Vertex> addV() {
        return __.<A>start().addV();
    }

    /**
     * @see {@link GraphTraversal#addV(Object...)}
     */
    @Deprecated
    public static <A> GraphTraversal<A, Vertex> addV(final Object... propertyKeyValues) {
        return __.<A>start().addV(propertyKeyValues);
    }

    /**
     * @see {@link GraphTraversal#addE(Direction, String, String, Object...)}
     */
    @Deprecated
    public static <A> GraphTraversal<A, Edge> addE(final Direction direction, final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        return __.<A>start().addE(direction, firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
    }

    /**
     * @see {@link GraphTraversal#addOutE(String, String, Object...)}
     */
    @Deprecated
    public static <A> GraphTraversal<A, Edge> addOutE(final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        return __.<A>start().addOutE(firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
    }

    /**
     * @see {@link GraphTraversal#addInE(String, String, Object...)}
     */
    @Deprecated
    public static <A> GraphTraversal<A, Edge> addInE(final String firstVertexKeyOrEdgeLabel, final String edgeLabelOrSecondVertexKey, final Object... propertyKeyValues) {
        return __.<A>start().addInE(firstVertexKeyOrEdgeLabel, edgeLabelOrSecondVertexKey, propertyKeyValues);
    }

    /**
     * @see {@link GraphTraversal#addE(String)}
     */
    public static <A> GraphTraversal<A, Edge> addE(final String edgeLabel) {
        return __.<A>start().addE(edgeLabel);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public static <A> GraphTraversal<A, A> filter(final Predicate<Traverser<A>> predicate) {
        return __.<A>start().filter(predicate);
    }

    public static <A> GraphTraversal<A, A> filter(final Traversal<?, ?> filterTraversal) {
        return __.<A>start().filter(filterTraversal);
    }

    public static <A> GraphTraversal<A, A> and(final Traversal<?, ?>... andTraversals) {
        return __.<A>start().and(andTraversals);
    }

    public static <A> GraphTraversal<A, A> or(final Traversal<?, ?>... orTraversals) {
        return __.<A>start().or(orTraversals);
    }

    public static <A> GraphTraversal<A, A> inject(final A... injections) {
        return __.<A>start().inject((A[]) injections);
    }

    public static <A> GraphTraversal<A, A> dedup(final String... dedupLabels) {
        return __.<A>start().dedup(dedupLabels);
    }

    public static <A> GraphTraversal<A, A> dedup(final Scope scope, final String... dedupLabels) {
        return __.<A>start().dedup(scope, dedupLabels);
    }

    public static <A> GraphTraversal<A, A> has(final String propertyKey, final P<?> predicate) {
        return __.<A>start().has(propertyKey, predicate);
    }

    public static <A> GraphTraversal<A, A> has(final T accessor, final P<?> predicate) {
        return __.<A>start().has(accessor, predicate);
    }

    public static <A> GraphTraversal<A, A> has(final String propertyKey, final Object value) {
        return __.<A>start().has(propertyKey, value);
    }

    public static <A> GraphTraversal<A, A> has(final T accessor, final Object value) {
        return __.<A>start().has(accessor, value);
    }

    public static <A> GraphTraversal<A, A> has(final String label, final String propertyKey, final Object value) {
        return __.<A>start().has(label, propertyKey, value);
    }

    public static <A> GraphTraversal<A, A> has(final String label, final String propertyKey, final P<?> predicate) {
        return __.<A>start().has(label, propertyKey, predicate);
    }

    public static <A> GraphTraversal<A, A> has(final String propertyKey, final Traversal<?, ?> propertyTraversal) {
        return __.<A>start().has(propertyKey, propertyTraversal);
    }

    public static <A> GraphTraversal<A, A> has(final String propertyKey) {
        return __.<A>start().has(propertyKey);
    }

    public static <A> GraphTraversal<A, A> hasNot(final String propertyKey) {
        return __.<A>start().hasNot(propertyKey);
    }

    public static <A> GraphTraversal<A, A> hasLabel(final String... labels) {
        return __.<A>start().hasLabel(labels);
    }

    public static <A> GraphTraversal<A, A> hasId(final Object... ids) {
        return __.<A>start().hasId(ids);
    }

    public static <A> GraphTraversal<A, A> hasKey(final String... keys) {
        return __.<A>start().hasKey(keys);
    }

    public static <A> GraphTraversal<A, A> hasValue(final Object... values) {
        return __.<A>start().hasValue(values);
    }

    public static <A> GraphTraversal<A, A> where(final String startKey, final P<String> predicate) {
        return __.<A>start().where(startKey, predicate);
    }

    public static <A> GraphTraversal<A, A> where(final P<String> predicate) {
        return __.<A>start().where(predicate);
    }

    public static <A> GraphTraversal<A, A> where(final Traversal<?, ?> whereTraversal) {
        return __.<A>start().where(whereTraversal);
    }

    public static <A> GraphTraversal<A, A> is(final P<A> predicate) {
        return __.<A>start().is(predicate);
    }

    public static <A> GraphTraversal<A, A> is(final Object value) {
        return __.<A>start().is(value);
    }

    public static <A> GraphTraversal<A, A> not(final Traversal<?, ?> notTraversal) {
        return __.<A>start().not(notTraversal);
    }

    public static <A> GraphTraversal<A, A> coin(final double probability) {
        return __.<A>start().coin(probability);
    }

    public static <A> GraphTraversal<A, A> range(final long low, final long high) {
        return __.<A>start().range(low, high);
    }

    public static <A> GraphTraversal<A, A> range(final Scope scope, final long low, final long high) {
        return __.<A>start().range(scope, low, high);
    }

    public static <A> GraphTraversal<A, A> limit(final long limit) {
        return __.<A>start().limit(limit);
    }

    public static <A> GraphTraversal<A, A> limit(final Scope scope, final long limit) {
        return __.<A>start().limit(scope, limit);
    }

    public static <A> GraphTraversal<A, A> tail() {
        return __.<A>start().tail();
    }

    public static <A> GraphTraversal<A, A> tail(final long limit) {
        return __.<A>start().tail(limit);
    }

    public static <A> GraphTraversal<A, A> tail(final Scope scope) {
        return __.<A>start().tail(scope);
    }

    public static <A> GraphTraversal<A, A> tail(final Scope scope, final long limit) {
        return __.<A>start().tail(scope, limit);
    }

    public static <A> GraphTraversal<A, A> simplePath() {
        return __.<A>start().simplePath();
    }

    public static <A> GraphTraversal<A, A> cyclicPath() {
        return __.<A>start().cyclicPath();
    }

    public static <A> GraphTraversal<A, A> sample(final int amountToSample) {
        return __.<A>start().sample(amountToSample);
    }

    public static <A> GraphTraversal<A, A> sample(final Scope scope, final int amountToSample) {
        return __.<A>start().sample(scope, amountToSample);
    }

    public static <A> GraphTraversal<A, A> drop() {
        return __.<A>start().drop();
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public static <A> GraphTraversal<A, A> sideEffect(final Consumer<Traverser<A>> consumer) {
        return __.<A>start().sideEffect(consumer);
    }

    public static <A> GraphTraversal<A, A> sideEffect(final Traversal<?, ?> sideEffectTraversal) {
        return __.<A>start().sideEffect(sideEffectTraversal);
    }

    public static <A, B> GraphTraversal<A, B> cap(final String sideEffectKey, String... sideEffectKeys) {
        return __.<A>start().cap(sideEffectKey, sideEffectKeys);
    }

    public static <A> GraphTraversal<A, Edge> subgraph(final String sideEffectKey) {
        return __.<A>start().subgraph(sideEffectKey);
    }

    public static <A> GraphTraversal<A, A> aggregate(final String sideEffectKey) {
        return __.<A>start().aggregate(sideEffectKey);
    }

    public static <A> GraphTraversal<A, A> group(final String sideEffectKey) {
        return __.<A>start().group(sideEffectKey);
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #group(String)}.
     */
    @Deprecated
    public static <A> GraphTraversal<A, A> groupV3d0(final String sideEffectKey) {
        return __.<A>start().groupV3d0(sideEffectKey);
    }

    public static <A> GraphTraversal<A, A> groupCount(final String sideEffectKey) {
        return __.<A>start().groupCount(sideEffectKey);
    }

    public static <A> GraphTraversal<A, A> timeLimit(final long timeLimit) {
        return __.<A>start().timeLimit(timeLimit);
    }

    public static <A> GraphTraversal<A, A> tree(final String sideEffectKey) {
        return __.<A>start().tree(sideEffectKey);
    }

    public static <A, V, U> GraphTraversal<A, A> sack(final BiFunction<V, U, V> sackOperator) {
        return __.<A>start().sack(sackOperator);
    }

    /**
     * @deprecated As of release 3.1.0, replaced by {@link #sack(BiFunction)}.
     */
    @Deprecated
    public static <A, V, U> GraphTraversal<A, A> sack(final BiFunction<V, U, V> sackOperator, final String elementPropertyKey) {
        return __.<A>start().sack(sackOperator, elementPropertyKey);
    }

    public static <A> GraphTraversal<A, A> store(final String sideEffectKey) {
        return __.<A>start().store(sideEffectKey);
    }

    public static <A> GraphTraversal<A, A> property(final Object key, final Object value, final Object... keyValues) {
        return __.<A>start().property(key, value, keyValues);
    }

    public static <A> GraphTraversal<A, A> property(final VertexProperty.Cardinality cardinality, final Object key, final Object value, final Object... keyValues) {
        return __.<A>start().property(cardinality, key, value, keyValues);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public static <A, M, B> GraphTraversal<A, B> branch(final Function<Traverser<A>, M> function) {
        return __.<A>start().branch(function);
    }

    public static <A, M, B> GraphTraversal<A, B> branch(final Traversal<?, M> traversalFunction) {
        return __.<A>start().branch(traversalFunction);
    }

    public static <A, B> GraphTraversal<A, B> choose(final Predicate<A> choosePredicate, final Traversal<?, B> trueChoice, final Traversal<?, B> falseChoice) {
        return __.<A>start().choose(choosePredicate, trueChoice, falseChoice);
    }

    public static <A, M, B> GraphTraversal<A, B> choose(final Function<A, M> choiceFunction) {
        return __.<A>start().choose(choiceFunction);
    }

    public static <A, M, B> GraphTraversal<A, B> choose(final Traversal<?, M> traversalFunction) {
        return __.<A>start().choose(traversalFunction);
    }

    public static <A, M, B> GraphTraversal<A, B> choose(final Traversal<?, M> traversalPredicate, final Traversal<?, B> trueChoice, final Traversal<?, B> falseChoice) {
        return __.<A>start().choose(traversalPredicate, trueChoice, falseChoice);
    }

    public static <A, B> GraphTraversal<A, B> union(final Traversal<?, B>... traversals) {
        return __.<A>start().union(traversals);
    }

    public static <A, B> GraphTraversal<A, B> coalesce(final Traversal<?, B>... traversals) {
        return __.<A>start().coalesce(traversals);
    }

    public static <A> GraphTraversal<A, A> repeat(final Traversal<?, A> traversal) {
        return __.<A>start().repeat(traversal);
    }

    public static <A> GraphTraversal<A, A> emit(final Traversal<?, ?> emitTraversal) {
        return __.<A>start().emit(emitTraversal);
    }

    public static <A> GraphTraversal<A, A> emit(final Predicate<Traverser<A>> emitPredicate) {
        return __.<A>start().emit(emitPredicate);
    }

    public static <A> GraphTraversal<A, A> until(final Traversal<?, ?> untilTraversal) {
        return __.<A>start().until(untilTraversal);
    }

    public static <A> GraphTraversal<A, A> until(final Predicate<Traverser<A>> untilPredicate) {
        return __.<A>start().until(untilPredicate);
    }

    public static <A> GraphTraversal<A, A> times(final int maxLoops) {
        return __.<A>start().times(maxLoops);
    }

    public static <A> GraphTraversal<A, A> emit() {
        return __.<A>start().emit();
    }

    public static <A, B> GraphTraversal<A, B> local(final Traversal<?, B> localTraversal) {
        return __.<A>start().local(localTraversal);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public static <A> GraphTraversal<A, A> as(final String label, final String... labels) {
        return __.<A>start().as(label, labels);
    }

    public static <A> GraphTraversal<A, A> barrier() {
        return __.<A>start().barrier();
    }

    public static <A> GraphTraversal<A, A> barrier(final int maxBarrierSize) {
        return __.<A>start().barrier(maxBarrierSize);
    }

    public static <A> GraphTraversal<A, A> barrier(final Consumer<TraverserSet<Object>> barrierConsumer) {
        return __.<A>start().barrier(barrierConsumer);
    }

}
