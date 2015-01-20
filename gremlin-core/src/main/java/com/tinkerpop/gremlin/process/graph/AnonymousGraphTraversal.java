package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public interface AnonymousGraphTraversal {

    public enum Tokens implements AnonymousGraphTraversal {
        __;

        public <S> GraphTraversal<S, S> start() {
            return new DefaultGraphTraversal<>(AnonymousGraphTraversal.class);
        }
    }

    //////////////////////////////////////////////////////////////////////

    public <A> GraphTraversal<A, A> start();

    //////////////////////////////////////////////////////////////////////

    public default <A> GraphTraversal<A, Long> count() {
        return this.<A>start().count();
    }

    public default <A> GraphTraversal<A, Double> sum() {
        return this.<A>start().sum();
    }

    public default <A> GraphTraversal<A, A> submit(final GraphComputer graphComputer) {
        return this.<A>start().submit(graphComputer);
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <A, E2> GraphTraversal<A, E2> map(final Function<Traverser<A>, E2> function) {
        return this.<A>start().map(function);
    }

    public default <A, E2> GraphTraversal<A, E2> flatMap(final Function<Traverser<A>, Iterator<E2>> function) {
        return this.<A>start().flatMap(function);
    }

    public default <A> GraphTraversal<A, A> identity() {
        return this.<A>start().identity();
    }

    public default <A> GraphTraversal<A, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.<A>start().to(direction, edgeLabels);
    }

    public default <A> GraphTraversal<A, Vertex> out(final String... edgeLabels) {
        return this.<A>start().out(edgeLabels);
    }

    public default <A> GraphTraversal<A, Vertex> in(final String... edgeLabels) {
        return this.<A>start().in(edgeLabels);
    }

    public default <A> GraphTraversal<A, Vertex> both(final String... edgeLabels) {
        return this.<A>start().both(edgeLabels);
    }

    public default <A> GraphTraversal<A, Edge> toE(final Direction direction, final String... edgeLabels) {
        return this.<A>start().toE(direction, edgeLabels);
    }

    public default <A> GraphTraversal<A, Edge> outE(final String... edgeLabels) {
        return this.<A>start().outE(edgeLabels);
    }

    public default <A> GraphTraversal<A, Edge> inE(final String... edgeLabels) {
        return this.<A>start().inE(edgeLabels);
    }

    public default <A> GraphTraversal<A, Edge> bothE(final String... edgeLabels) {
        return this.<A>start().bothE(edgeLabels);
    }

    public default <A> GraphTraversal<A, Vertex> toV(final Direction direction) {
        return this.<A>start().toV(direction);
    }

    public default <A> GraphTraversal<A, Vertex> inV() {
        return this.<A>start().inV();
    }

    public default <A> GraphTraversal<A, Vertex> outV() {
        return this.<A>start().outV();
    }

    public default <A> GraphTraversal<A, Vertex> bothV() {
        return this.<A>start().bothV();
    }

    public default <A> GraphTraversal<A, Vertex> otherV() {
        return this.<A>start().otherV();
    }

    public default <A> GraphTraversal<A, A> order() {
        return this.<A>start().order();
    }

    public default <A> GraphTraversal<A, A> shuffle() {
        return this.<A>start().shuffle();
    }

    public default <A, E2> GraphTraversal<A, ? extends Property<E2>> properties(final String... propertyKeys) {
        return this.<A>start().properties(propertyKeys);
    }

    public default <A, E2> GraphTraversal<A, E2> values(final String... propertyKeys) {
        return this.<A>start().values(propertyKeys);
    }

    public default <A, E2> GraphTraversal<A, Map<String, E2>> propertyMap(final String... propertyKeys) {
        return this.<A>start().propertyMap(propertyKeys);
    }

    public default <A, E2> GraphTraversal<A, Map<String, E2>> valueMap(final String... propertyKeys) {
        return this.<A>start().valueMap(propertyKeys);
    }

    public default <A, E2> GraphTraversal<A, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return this.<A>start().valueMap(includeTokens, propertyKeys);
    }

    public default <A, E2> GraphTraversal<A, E2> value() {
        return this.<A>start().value();
    }

    public default <A> GraphTraversal<A, Path> path() {
        return this.<A>start().path();
    }

    public default <A, E2> GraphTraversal<A, E2> back(final String stepLabel) {
        return this.<A>start().back(stepLabel);
    }

    public default <A, E2> GraphTraversal<A, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return this.<A>start().match(startLabel, traversals);
    }

    public default <A, E2> GraphTraversal<A, E2> sack() {
        return this.<A>start().sack();
    }

    public default <A, E2> GraphTraversal<A, E2> select(final String stepLabel) {
        return this.<A>start().select(stepLabel);
    }

    public default <A, E2> GraphTraversal<A, Map<String, E2>> select(final String... stepLabels) {
        return this.<A>start().select(stepLabels);
    }

    public default <A> GraphTraversal<A, A> unfold() {
        return this.<A>start().unfold();
    }

    public default <A> GraphTraversal<A, List<A>> fold() {
        return this.<A>start().fold();
    }

    public default <A, E2> GraphTraversal<A, E2> fold(final E2 seed, final BiFunction<E2, A, E2> foldFunction) {
        return this.<A>start().fold(seed, foldFunction);
    }

    public default <A, E2> GraphTraversal<A, E2> local(final Traversal<?, E2> localTraversal) {
        return this.<A>start().local(localTraversal);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default <A> GraphTraversal<A, A> filter(final Predicate<Traverser<A>> predicate) {
        return this.<A>start().filter(predicate);
    }

    public default <A> GraphTraversal<A, A> inject(final Object... injections) {
        return this.<A>start().inject((A[]) injections);
    }

    public default <A> GraphTraversal<A, A> dedup() {
        return this.<A>start().dedup();
    }

    public default <A> GraphTraversal<A, A> except(final String sideEffectKey) {
        return this.<A>start().except(sideEffectKey);
    }

    public default <A> GraphTraversal<A, A> except(final Object exceptionObject) {
        return this.<A>start().except((A) exceptionObject);
    }

    public default <A> GraphTraversal<A, A> except(final Collection<A> exceptionCollection) {
        return this.<A>start().except(exceptionCollection);
    }

    public default <A extends Element> GraphTraversal<A, A> has(final String key) {
        return this.<A>start().has(key);
    }

    public default <A extends Element> GraphTraversal<A, A> has(final String key, final Object value) {
        return this.<A>start().has(key, value);
    }

    public default <A extends Element> GraphTraversal<A, A> has(final T accessor, final Object value) {
        return this.<A>start().has(accessor, value);
    }

    public default <A extends Element> GraphTraversal<A, A> has(final String key, final BiPredicate predicate, final Object value) {
        return this.<A>start().has(key, predicate, value);
    }

    public default <A extends Element> GraphTraversal<A, A> has(final T accessor, final BiPredicate predicate, final Object value) {
        return this.<A>start().has(accessor, predicate, value);
    }

    public default <A extends Element> GraphTraversal<A, A> has(final String label, final String key, final Object value) {
        return this.<A>start().has(label, key, value);
    }

    public default <A extends Element> GraphTraversal<A, A> has(final String label, final String key, final BiPredicate predicate, final Object value) {
        return this.<A>start().has(label, key, predicate, value);
    }

    public default <A extends Element> GraphTraversal<A, A> hasNot(final String key) {
        return this.<A>start().hasNot(key);
    }

    public default <A, E2> GraphTraversal<A, Map<String, E2>> where(final String firstKey, final String secondKey, final BiPredicate predicate) {
        return this.<A>start().where(firstKey, secondKey, predicate);
    }

    public default <A, E2> GraphTraversal<A, Map<String, E2>> where(final String firstKey, final BiPredicate predicate, final String secondKey) {
        return this.<A>start().where(firstKey, predicate, secondKey);
    }

    public default <A, E2> GraphTraversal<A, Map<String, E2>> where(final Traversal constraint) {
        return this.<A>start().where(constraint);
    }

    public default <A extends Element> GraphTraversal<A, A> between(final String key, final Comparable startValue, final Comparable endValue) {
        return this.<A>start().between(key, startValue, endValue);
    }

    public default <A> GraphTraversal<A, A> coin(final double probability) {
        return this.<A>start().coin(probability);
    }

    public default <A> GraphTraversal<A, A> range(final long low, final long high) {
        return this.<A>start().range(low, high);
    }

    public default <A> GraphTraversal<A, A> limit(final long limit) {
        return this.<A>start().limit(limit);
    }

    public default <A> GraphTraversal<A, A> retain(final String sideEffectKey) {
        return this.<A>start().retain(sideEffectKey);
    }

    public default <A> GraphTraversal<A, A> retain(final Object retainObject) {
        return this.<A>start().retain((A) retainObject);
    }

    public default <A> GraphTraversal<A, A> retain(final Collection<A> retainCollection) {
        return this.<A>start().retain(retainCollection);
    }

    public default <A> GraphTraversal<A, A> simplePath() {
        return this.<A>start().simplePath();
    }

    public default <A> GraphTraversal<A, A> cyclicPath() {
        return this.<A>start().cyclicPath();
    }

    public default <A> GraphTraversal<A, A> sample(final int amountToSample) {
        return this.<A>start().sample(amountToSample);
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default <A> GraphTraversal<A, A> sideEffect(final Consumer<Traverser<A>> consumer) {
        return this.<A>start().sideEffect(consumer);
    }

    public default <A, E2> GraphTraversal<A, E2> cap(final String sideEffectKey) {
        return this.<A>start().cap(sideEffectKey);
    }

    public default <A, E2> GraphTraversal<A, E2> cap() {
        return this.<A>start().cap();
    }

    public default <A> GraphTraversal<A, A> subgraph(final String sideEffectKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final Predicate<Edge> includeEdge) {
        return this.<A>start().subgraph(sideEffectKey, edgeIdHolder, vertexMap, includeEdge);
    }

    public default <A> GraphTraversal<A, A> subgraph(final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final Predicate<Edge> includeEdge) {
        return this.<A>start().subgraph(edgeIdHolder, vertexMap, includeEdge);
    }

    public default <A> GraphTraversal<A, A> subgraph(final String sideEffectKey, final Predicate<Edge> includeEdge) {
        return this.<A>start().subgraph(sideEffectKey, includeEdge);
    }

    public default <A> GraphTraversal<A, A> subgraph(final Predicate<Edge> includeEdge) {
        return this.<A>start().subgraph(includeEdge);
    }

    public default <A> GraphTraversal<A, A> aggregate(final String sideEffectKey) {
        return this.<A>start().aggregate(sideEffectKey);
    }

    public default <A> GraphTraversal<A, A> aggregate() {
        return this.<A>start().aggregate();
    }

    public default <A> GraphTraversal<A, A> group(final String sideEffectKey) {
        return this.<A>start().group(sideEffectKey);
    }

    public default <A> GraphTraversal<A, A> group() {
        return this.<A>start().group();
    }

    public default <A> GraphTraversal<A, A> groupCount(final String sideEffectKey) {
        return this.<A>start().groupCount(sideEffectKey);
    }

    public default <A> GraphTraversal<A, A> groupCount() {
        return this.<A>start().groupCount();
    }

    public default <A> GraphTraversal<A, Vertex> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.<A>start().addE(direction, edgeLabel, stepLabel, propertyKeyValues);
    }

    public default <A> GraphTraversal<A, Vertex> addInE(final String edgeLabel, final String setLabel, final Object... propertyKeyValues) {
        return this.<A>start().addInE(edgeLabel, setLabel, propertyKeyValues);
    }

    public default <A> GraphTraversal<A, Vertex> addOutE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.<A>start().addOutE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public default <A> GraphTraversal<A, Vertex> addBothE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.<A>start().addBothE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public default <A> GraphTraversal<A, A> timeLimit(final long timeLimit) {
        return this.<A>start().timeLimit(timeLimit);
    }

    public default <A> GraphTraversal<A, A> tree(final String sideEffectKey) {
        return this.<A>start().tree(sideEffectKey);
    }

    public default <A> GraphTraversal<A, A> tree() {
        return this.<A>start().tree();
    }

    public default <A, V> GraphTraversal<A, A> sack(final BiFunction<V, A, V> sackFunction) {
        return this.<A>start().sack(sackFunction);
    }

    public default <A extends Element, V> GraphTraversal<A, A> sack(final BinaryOperator<V> sackOperator, final String elementPropertyKey) {
        return this.<A>start().sack(sackOperator, elementPropertyKey);
    }

    public default <A> GraphTraversal<A, A> store(final String sideEffectKey) {
        return this.<A>start().store(sideEffectKey);
    }

    public default <A> GraphTraversal<A, A> store() {
        return this.<A>start().store();
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default <A> GraphTraversal<A, A> branch(final Function<Traverser<A>, Collection<String>> function) {
        return this.<A>start().branch(function);
    }

    public default <A, E2> GraphTraversal<A, E2> choose(final Predicate<A> choosePredicate, final Traversal<?, E2> trueChoice, final Traversal<?, E2> falseChoice) {
        return this.<A>start().choose(choosePredicate, trueChoice, falseChoice);
    }

    public default <A, E2, M> GraphTraversal<A, E2> choose(final Function<A, M> mapFunction, final Map<M, Traversal<?, E2>> choices) {
        return this.<A>start().choose(mapFunction, choices);
    }

    public default <A, E2> GraphTraversal<A, E2> union(final Traversal<?, E2>... traversals) {
        return this.<A>start().union(traversals);
    }

    public default <A> GraphTraversal<A, A> repeat(final Traversal<?, A> traversal) {
        return this.<A>start().repeat(traversal);
    }

    public default <A> GraphTraversal<A, A> emit(final Predicate<Traverser<A>> emitPredicate) {
        return this.<A>start().emit(emitPredicate);
    }

    public default <A> GraphTraversal<A, A> until(final Predicate<Traverser<A>> untilPredicate) {
        return this.<A>start().until(untilPredicate);
    }

    public default <A> GraphTraversal<A, A> times(final int maxLoops) {
        return this.<A>start().times(maxLoops);
    }

    public default <A> GraphTraversal<A, A> emit() {
        return this.<A>start().emit();
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default <A> GraphTraversal<A, A> as(final String label) {
        return this.<A>start().as(label);
    }

    public default <A> GraphTraversal<A, A> profile() {
        return this.<A>start().profile();
    }

    public default <A> GraphTraversal<A, A> withSideEffect(final String key, final Supplier supplier) {
        return this.<A>start().withSideEffect(key, supplier);
    }

    public default <A, B> GraphTraversal<A, A> withSack(final Supplier<B> initialValue, final UnaryOperator<B> splitOperator) {
        return this.<A>start().withSack(initialValue, splitOperator);
    }

    public default <A, B> GraphTraversal<A, A> withSack(final Supplier<B> initialValue) {
        return this.<A>start().withSack(initialValue);
    }

    public default <A> GraphTraversal<A, A> withPath() {
        return this.<A>start().withPath();
    }
}


