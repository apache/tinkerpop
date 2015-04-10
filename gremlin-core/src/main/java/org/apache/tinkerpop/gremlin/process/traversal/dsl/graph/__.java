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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class __ {

    protected __() {
    }

    //////////////////////////////////////////////////////////////////////

    public static <A> GraphTraversal<A, A> start() {
        return new DefaultGraphTraversal<>(EmptyGraph.instance());
    }

    public static <A> GraphTraversal<A, A> __(final Object... starts) {
        return inject(starts);
    }

    ///////////////////// MAP STEPS /////////////////////

    public static <A, E2> GraphTraversal<A, E2> map(final Function<Traverser<A>, E2> function) {
        return __.<A>start().map(function);
    }

    public static <A, E2> GraphTraversal<A, E2> flatMap(final Function<Traverser<A>, Iterator<E2>> function) {
        return __.<A>start().flatMap(function);
    }

    public static <A> GraphTraversal<A, A> identity() {
        return __.<A>start().identity();
    }

    public static <A> GraphTraversal<A, String> label() {
        return __.<A>start().label();
    }

    public static <A> GraphTraversal<A, Object> id() {
        return __.<A>start().id();
    }

    public static <A> GraphTraversal<A, Vertex> to(final Direction direction, final String... edgeLabels) {
        return __.<A>start().to(direction, edgeLabels);
    }

    public static <A> GraphTraversal<A, Vertex> out(final String... edgeLabels) {
        return __.<A>start().out(edgeLabels);
    }

    public static <A> GraphTraversal<A, Vertex> in(final String... edgeLabels) {
        return __.<A>start().in(edgeLabels);
    }

    public static <A> GraphTraversal<A, Vertex> both(final String... edgeLabels) {
        return __.<A>start().both(edgeLabels);
    }

    public static <A> GraphTraversal<A, Edge> toE(final Direction direction, final String... edgeLabels) {
        return __.<A>start().toE(direction, edgeLabels);
    }

    public static <A> GraphTraversal<A, Edge> outE(final String... edgeLabels) {
        return __.<A>start().outE(edgeLabels);
    }

    public static <A> GraphTraversal<A, Edge> inE(final String... edgeLabels) {
        return __.<A>start().inE(edgeLabels);
    }

    public static <A> GraphTraversal<A, Edge> bothE(final String... edgeLabels) {
        return __.<A>start().bothE(edgeLabels);
    }

    public static <A> GraphTraversal<A, Vertex> toV(final Direction direction) {
        return __.<A>start().toV(direction);
    }

    public static <A> GraphTraversal<A, Vertex> inV() {
        return __.<A>start().inV();
    }

    public static <A> GraphTraversal<A, Vertex> outV() {
        return __.<A>start().outV();
    }

    public static <A> GraphTraversal<A, Vertex> bothV() {
        return __.<A>start().bothV();
    }

    public static <A> GraphTraversal<A, Vertex> otherV() {
        return __.<A>start().otherV();
    }

    public static <A> GraphTraversal<A, A> order() {
        return __.<A>start().order();
    }

    public static <A> GraphTraversal<A, A> order(final Scope scope) {
        return __.<A>start().order(scope);
    }

    public static <A, E2> GraphTraversal<A, ? extends Property<E2>> properties(final String... propertyKeys) {
        return __.<A>start().properties(propertyKeys);
    }

    public static <A, E2> GraphTraversal<A, E2> values(final String... propertyKeys) {
        return __.<A>start().values(propertyKeys);
    }

    public static <A, E2> GraphTraversal<A, Map<String, E2>> propertyMap(final String... propertyKeys) {
        return __.<A>start().propertyMap(propertyKeys);
    }

    public static <A, E2> GraphTraversal<A, Map<String, E2>> valueMap(final String... propertyKeys) {
        return __.<A>start().valueMap(propertyKeys);
    }

    public static <A, E2> GraphTraversal<A, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return __.<A>start().valueMap(includeTokens, propertyKeys);
    }

    public static <A> GraphTraversal<A, String> key() {
        return __.<A>start().key();
    }

    public static <A, E2> GraphTraversal<A, E2> value() {
        return __.<A>start().value();
    }

    public static <A> GraphTraversal<A, Path> path() {
        return __.<A>start().path();
    }

    public static <A, E2> GraphTraversal<A, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return __.<A>start().match(startLabel, traversals);
    }

    public static <A, E2> GraphTraversal<A, E2> sack() {
        return __.<A>start().sack();
    }

    public static <A, E2> GraphTraversal<A, E2> select(final String stepLabel) {
        return __.<A>start().select(stepLabel);
    }

    public static <A, E2> GraphTraversal<A, Map<String, E2>> select(final String... stepLabels) {
        return __.<A>start().select(stepLabels);
    }

    public static <A> GraphTraversal<A, A> unfold() {
        return __.<A>start().unfold();
    }

    public static <A> GraphTraversal<A, List<A>> fold() {
        return __.<A>start().fold();
    }

    public static <A, E2> GraphTraversal<A, E2> fold(final E2 seed, final BiFunction<E2, A, E2> foldFunction) {
        return __.<A>start().fold(seed, foldFunction);
    }

    public static <A> GraphTraversal<A, Long> count() {
        return __.<A>start().count();
    }

    public static <A> GraphTraversal<A, Long> count(final Scope scope) {
        return __.<A>start().count(scope);
    }

    public static <A> GraphTraversal<A, Double> sum() {
        return __.<A>start().sum();
    }

    public static <A> GraphTraversal<A, Double> sum(final Scope scope) {
        return __.<A>start().sum(scope);
    }


    public static <A, E2 extends Number> GraphTraversal<A, E2> min() {
        return __.<A>start().min();
    }

    public static <A, E2 extends Number> GraphTraversal<A, E2> min(final Scope scope) {
        return __.<A>start().min(scope);
    }

    public static <A, E2 extends Number> GraphTraversal<A, E2> max() {
        return __.<A>start().max();
    }

    public static <A, E2 extends Number> GraphTraversal<A, E2> max(final Scope scope) {
        return __.<A>start().max(scope);
    }

    public static <A> GraphTraversal<A, Double> mean() {
        return __.<A>start().mean();
    }

    public static <A> GraphTraversal<A, Double> mean(final Scope scope) {
        return __.<A>start().mean(scope);
    }

    public static <A, K, R> GraphTraversal<A, Map<K, R>> group() {
        return __.<A>start().group();
    }

    public static <A, B> GraphTraversal<A, Map<B, Long>> groupCount() {
        return __.<A>start().<B>groupCount();
    }

    public static <A> GraphTraversal<A, Tree> tree() {
        return __.<A>start().tree();
    }

    public static <A> GraphTraversal<A, Vertex> addV(final Object... propertyKeyValues) {
        return __.<A>start().addV(propertyKeyValues);
    }

    public static <A> GraphTraversal<A, Edge> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return __.<A>start().addE(direction, edgeLabel, stepLabel, propertyKeyValues);
    }

    public static <A> GraphTraversal<A, Edge> addInE(final String edgeLabel, final String setLabel, final Object... propertyKeyValues) {
        return __.<A>start().addInE(edgeLabel, setLabel, propertyKeyValues);
    }

    public static <A> GraphTraversal<A, Edge> addOutE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return __.<A>start().addOutE(edgeLabel, stepLabel, propertyKeyValues);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public static <A> GraphTraversal<A, A> filter(final Predicate<Traverser<A>> predicate) {
        return __.<A>start().filter(predicate);
    }

    public static <A> GraphTraversal<A, A> and(final Traversal<?, ?>... andTraversals) {
        return __.<A>start().and(andTraversals);
    }

    public static <A> GraphTraversal<A, A> or(final Traversal<?, ?>... orTraversals) {
        return __.<A>start().or(orTraversals);
    }

    public static <A> GraphTraversal<A, A> inject(final Object... injections) {
        return __.<A>start().inject((A[]) injections);
    }

    public static <A> GraphTraversal<A, A> dedup() {
        return __.<A>start().dedup();
    }

    public static <A> GraphTraversal<A, A> dedup(final Scope scope) {
        return __.<A>start().dedup(scope);
    }

    public static <A> GraphTraversal<A, A> except(final String sideEffectKeyOrPathLabel) {
        return __.<A>start().except(sideEffectKeyOrPathLabel);
    }

    public static <A> GraphTraversal<A, A> except(final Object exceptObject) {
        return __.<A>start().except((A) exceptObject);
    }

    public static <A> GraphTraversal<A, A> except(final Collection<A> exceptCollection) {
        return __.<A>start().except(exceptCollection);
    }

    public static <A> GraphTraversal<A, A> has(final Traversal<?, ?> hasNextTraversal) {
        return __.<A>start().has(hasNextTraversal);
    }

    public static <A> GraphTraversal<A, A> hasNot(final Traversal<?, ?> hasNotNextTraversal) {
        return __.<A>start().hasNot(hasNotNextTraversal);
    }

    public static <A> GraphTraversal<A, A> has(final String key) {
        return __.<A>start().has(key);
    }

    public static <A> GraphTraversal<A, A> has(final String key, final Object value) {
        return __.<A>start().has(key, value);
    }

    public static <A> GraphTraversal<A, A> has(final T accessor, final Object value) {
        return __.<A>start().has(accessor, value);
    }

    public static <A> GraphTraversal<A, A> has(final String key, final BiPredicate predicate, final Object value) {
        return __.<A>start().has(key, predicate, value);
    }

    public static <A> GraphTraversal<A, A> has(final T accessor, final BiPredicate predicate, final Object value) {
        return __.<A>start().has(accessor, predicate, value);
    }

    public static <A> GraphTraversal<A, A> has(final String label, final String key, final Object value) {
        return __.<A>start().has(label, key, value);
    }

    public static <A> GraphTraversal<A, A> has(final String label, final String key, final BiPredicate predicate, final Object value) {
        return __.<A>start().has(label, key, predicate, value);
    }

    public static <A> GraphTraversal<A, A> hasNot(final String key) {
        return __.<A>start().hasNot(key);
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

    public static <A, E2> GraphTraversal<A, Map<String, E2>> where(final String firstKey, final String secondKey, final BiPredicate predicate) {
        return __.<A>start().where(firstKey, secondKey, predicate);
    }

    public static <A, E2> GraphTraversal<A, Map<String, E2>> where(final String firstKey, final BiPredicate predicate, final String secondKey) {
        return __.<A>start().where(firstKey, predicate, secondKey);
    }

    public static <A, E2> GraphTraversal<A, Map<String, E2>> where(final Traversal constraint) {
        return __.<A>start().where(constraint);
    }

    public static <A> GraphTraversal<A, A> is(final Object value) {
        return __.<A>start().is(value);
    }

    public static <A> GraphTraversal<A, A> is(final BiPredicate predicate, final Object value) {
        return __.<A>start().is(predicate, value);
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

    public static <A> GraphTraversal<A, A> retain(final String sideEffectKeyOrPathLabel) {
        return __.<A>start().retain(sideEffectKeyOrPathLabel);
    }

    public static <A> GraphTraversal<A, A> retain(final Object retainObject) {
        return __.<A>start().retain((A) retainObject);
    }

    public static <A> GraphTraversal<A, A> retain(final Collection<A> retainCollection) {
        return __.<A>start().retain(retainCollection);
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

    public static <A, E2> GraphTraversal<A, E2> cap(final String sideEffectKey, String... sideEffectKeys) {
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

    public static <A> GraphTraversal<A, A> groupCount(final String sideEffectKey) {
        return __.<A>start().groupCount(sideEffectKey);
    }

    public static <A> GraphTraversal<A, A> timeLimit(final long timeLimit) {
        return __.<A>start().timeLimit(timeLimit);
    }

    public static <A> GraphTraversal<A, A> tree(final String sideEffectKey) {
        return __.<A>start().tree(sideEffectKey);
    }

    public static <A, V> GraphTraversal<A, A> sack(final BiFunction<V, A, V> sackFunction) {
        return __.<A>start().sack(sackFunction);
    }

    public static <A, V> GraphTraversal<A, A> sack(final BinaryOperator<V> sackOperator, final String elementPropertyKey) {
        return __.<A>start().sack(sackOperator, elementPropertyKey);
    }

    public static <A> GraphTraversal<A, A> store(final String sideEffectKey) {
        return __.<A>start().store(sideEffectKey);
    }

    public static <A> GraphTraversal<A, A> property(final String key, final Object value, final Object... keyValues) {
        return __.<A>start().property(key, value, keyValues);
    }

    public static <A> GraphTraversal<A, A> property(final VertexProperty.Cardinality cardinality, final String key, final Object value, final Object... keyValues) {
        return __.<A>start().property(cardinality, key, value, keyValues);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public static <A, M, E2> GraphTraversal<A, E2> branch(final Function<Traverser<A>, M> function) {
        return __.<A>start().branch(function);
    }

    public static <A, M, E2> GraphTraversal<A, E2> branch(final Traversal<?, M> traversalFunction) {
        return __.<A>start().branch(traversalFunction);
    }

    public static <A, E2> GraphTraversal<A, E2> choose(final Predicate<A> choosePredicate, final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        return __.<A>start().choose(choosePredicate, trueChoice, falseChoice);
    }

    public static <A, M, E2> GraphTraversal<A, E2> choose(final Function<A, M> choiceFunction) {
        return __.<A>start().choose(choiceFunction);
    }

    public static <A, M, E2> GraphTraversal<A, E2> choose(final Traversal<?, M> traversalFunction) {
        return __.<A>start().choose(traversalFunction);
    }

    public static <A, M, E2> GraphTraversal<A, E2> choose(final Traversal<?, M> traversalPredicate, final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        return __.<A>start().choose(traversalPredicate, trueChoice, falseChoice);
    }

    public static <A, E2> GraphTraversal<A, E2> union(final Traversal<?, E2>... traversals) {
        return __.<A>start().union(traversals);
    }

    public static <A, E2> GraphTraversal<A, E2> coalesce(final Traversal<?, E2>... traversals) {
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

    public static <A, E2> GraphTraversal<A, E2> local(final Traversal<?, E2> localTraversal) {
        return __.<A>start().local(localTraversal);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public static <A> GraphTraversal<A, A> as(final String label) {
        return __.<A>start().as(label);
    }

    public static <A> GraphTraversal<A, A> profile() {
        return __.<A>start().profile();
    }

    public static <A> GraphTraversal<A, A> withSideEffect(final String key, final Supplier supplier) {
        return __.<A>start().withSideEffect(key, supplier);
    }

    public static <A, B> GraphTraversal<A, A> withSack(final Supplier<B> initialValue, final UnaryOperator<B> splitOperator) {
        return __.<A>start().withSack(initialValue, splitOperator);
    }

    public static <A, B> GraphTraversal<A, A> withSack(final Supplier<B> initialValue) {
        return __.<A>start().withSack(initialValue);
    }

    public static <A, B> GraphTraversal<A, A> withSack(final B initialValue, final UnaryOperator<B> splitOperator) {
        return __.<A>start().withSack(initialValue, splitOperator);
    }

    public static <A, B> GraphTraversal<A, A> withSack(B initialValue) {
        return __.<A>start().withSack(initialValue);
    }

    public static <A> GraphTraversal<A, A> withPath() {
        return __.<A>start().withPath();
    }

    public static <A> GraphTraversal<A, A> barrier() {
        return __.<A>start().barrier();
    }

    ////

    public static <A> GraphTraversal<A, A> iterate() {
        return __.<A>start().iterate();
    }


}
