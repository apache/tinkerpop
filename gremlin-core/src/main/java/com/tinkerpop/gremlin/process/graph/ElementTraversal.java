package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SBiConsumer;
import com.tinkerpop.gremlin.util.function.SBiFunction;
import com.tinkerpop.gremlin.util.function.SBiPredicate;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract interface ElementTraversal<A extends Element> {

    //////////////////////////////////////////////////////////////////////

    public default GraphTraversal<A, A> start() {
        final GraphTraversal<A, A> traversal = GraphTraversal.of();
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    //////////////////////////////////////////////////////////////////////

    public default GraphTraversal<A, A> trackPaths() {
        return this.start().trackPaths();
    }

    public default GraphTraversal<A, Long> count() {
        return this.start().count();
    }

    public default GraphTraversal<A, A> submit(final GraphComputer graphComputer) {
        return this.start().submit(graphComputer);
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> GraphTraversal<A, E2> map(final SFunction<Traverser<A>, E2> function) {
        return this.start().map(function);
    }

    public default <E2> GraphTraversal<A, E2> map(final SBiFunction<Traverser<A>, Traversal.SideEffects, E2> biFunction) {
        return this.start().map(biFunction);
    }

    public default <E2> GraphTraversal<A, E2> flatMap(final SFunction<Traverser<A>, Iterator<E2>> function) {
        return this.start().flatMap(function);
    }

    public default <E2> GraphTraversal<A, E2> flatMap(final SBiFunction<Traverser<A>, Traversal.SideEffects, Iterator<E2>> biFunction) {
        return this.start().flatMap(biFunction);
    }

    public default GraphTraversal<A, A> identity() {
        return this.start().identity();
    }

    public default GraphTraversal<A, Vertex> to(final Direction direction, final int branchFactor, final String... edgeLabels) {
        return this.start().to(direction, branchFactor, edgeLabels);
    }

    public default GraphTraversal<A, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.start().to(direction, edgeLabels);
    }

    public default GraphTraversal<A, Vertex> out(final int branchFactor, final String... edgeLabels) {
        return this.start().out(branchFactor, edgeLabels);
    }

    public default GraphTraversal<A, Vertex> out(final String... edgeLabels) {
        return this.start().out(edgeLabels);
    }

    public default GraphTraversal<A, Vertex> in(final int branchFactor, final String... edgeLabels) {
        return this.start().in(branchFactor, edgeLabels);
    }

    public default GraphTraversal<A, Vertex> in(final String... edgeLabels) {
        return this.start().in(edgeLabels);
    }

    public default GraphTraversal<A, Vertex> both(final int branchFactor, final String... edgeLabels) {
        return this.start().both(branchFactor, edgeLabels);
    }

    public default GraphTraversal<A, Vertex> both(final String... edgeLabels) {
        return this.start().both(edgeLabels);
    }

    public default GraphTraversal<A, Edge> toE(final Direction direction, final int branchFactor, final String... edgeLabels) {
        return this.start().toE(direction, branchFactor, edgeLabels);
    }

    public default GraphTraversal<A, Edge> toE(final Direction direction, final String... edgeLabels) {
        return this.start().toE(direction, edgeLabels);
    }

    public default GraphTraversal<A, Edge> outE(final int branchFactor, final String... edgeLabels) {
        return this.start().outE(branchFactor, edgeLabels);
    }

    public default GraphTraversal<A, Edge> outE(final String... edgeLabels) {
        return this.start().outE(edgeLabels);
    }

    public default GraphTraversal<A, Edge> inE(final int branchFactor, final String... edgeLabels) {
        return this.start().inE(branchFactor, edgeLabels);
    }

    public default GraphTraversal<A, Edge> inE(final String... edgeLabels) {
        return this.start().inE(edgeLabels);
    }

    public default GraphTraversal<A, Edge> bothE(final int branchFactor, final String... edgeLabels) {
        return this.start().bothE(branchFactor, edgeLabels);
    }

    public default GraphTraversal<A, Edge> bothE(final String... edgeLabels) {
        return this.start().bothE(edgeLabels);
    }

    public default GraphTraversal<A, Vertex> toV(final Direction direction) {
        return this.start().toV(direction);
    }

    public default GraphTraversal<A, Vertex> inV() {
        return this.start().inV();
    }

    public default GraphTraversal<A, Vertex> outV() {
        return this.start().outV();
    }

    public default GraphTraversal<A, Vertex> bothV() {
        return this.start().bothV();
    }

    public default GraphTraversal<A, Vertex> otherV() {
        return this.start().otherV();
    }

    public default GraphTraversal<A, A> order() {
        return this.start().order();
    }

    public default GraphTraversal<A, A> order(final Comparator<Traverser<A>> comparator) {
        return this.start().order(comparator);
    }

    public default GraphTraversal<A, A> shuffle() {
        return this.start().shuffle();
    }

    public default <E2> GraphTraversal<A, ? extends Property<E2>> properties(final String... propertyKeys) {
        return this.start().properties(propertyKeys);
    }

    public default <E2> GraphTraversal<A, ? extends Property<E2>> hiddens(final String... propertyKeys) {
        return this.start().hiddens(propertyKeys);
    }

    public default <E2> GraphTraversal<A, E2> hiddenValue(final String propertyKey) {
        return this.start().hiddenValue(propertyKey);
    }

    public default <E2> GraphTraversal<A, E2> hiddenValue(final String propertyKey, final E2 defaultValue) {
        return this.start().hiddenValue(propertyKey, defaultValue);
    }

    public default <E2> GraphTraversal<A, E2> hiddenValue(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.start().hiddenValue(propertyKey, defaultSupplier);
    }

    public default <E2> GraphTraversal<A, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.start().value(propertyKey, defaultSupplier);
    }

    public default <E2> GraphTraversal<A, E2> values(final String... propertyKeys) {
        return this.start().values(propertyKeys);
    }

    public default GraphTraversal<A, Path> path(final SFunction... pathFunctions) {
        return this.start().path(pathFunctions);
    }

    public default <E2> GraphTraversal<A, E2> back(final String stepLabel) {
        return this.start().back(stepLabel);
    }

    public default <E2> GraphTraversal<A, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return this.start().match(startLabel, traversals);
    }

    public default <E2> GraphTraversal<A, Map<String, E2>> select(final List<String> labels, SFunction... stepFunctions) {
        return this.start().select(labels, stepFunctions);
    }

    public default <E2> GraphTraversal<A, Map<String, E2>> select(final SFunction... stepFunctions) {
        return this.start().select(stepFunctions);
    }

    public default <E2> GraphTraversal<A, E2> select(final String label, SFunction stepFunction) {
        return this.start().select(label, stepFunction);
    }

    public default <E2> GraphTraversal<A, E2> select(final String label) {
        return this.start().select(label, null);
    }

    /*public default <E2> GraphTraversal<S, E2> union(final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new UnionStep(this, traversals));
    }*/

    /*public default <E2> GraphTraversal<S, E2> intersect(final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new IntersectStep(this, traversals));
    }*/

    public default GraphTraversal<A, A> unfold() {
        return this.start().unfold();
    }

    public default GraphTraversal<A, List<A>> fold() {
        return this.start().fold();
    }

    public default <E2> GraphTraversal<A, E2> fold(final E2 seed, final SBiFunction<E2, Traverser<A>, E2> foldFunction) {
        return this.start().fold(seed, foldFunction);
    }

    public default <E2> GraphTraversal<A, E2> choose(final SPredicate<Traverser<A>> choosePredicate, final Traversal trueChoice, final Traversal falseChoice) {
        return this.start().choose(choosePredicate, trueChoice, falseChoice);
    }

    public default <E2, M> GraphTraversal<A, E2> choose(final SFunction<Traverser<A>, M> mapFunction, final Map<M, Traversal<A, E2>> choices) {
        return this.start().choose(mapFunction, choices);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default GraphTraversal<A, A> filter(final SPredicate<Traverser<A>> predicate) {
        return this.start().filter(predicate);
    }

    public default GraphTraversal<A, A> filter(final SBiPredicate<Traverser<A>, Traversal.SideEffects> biPredicate) {
        return this.start().filter(biPredicate);
    }

    public default GraphTraversal<A, A> inject(final Object... injections) {
        return this.start().inject((A[]) injections);
    }

    public default GraphTraversal<A, A> dedup() {
        return this.start().dedup();
    }

    public default GraphTraversal<A, A> dedup(final SFunction<Traverser<A>, ?> uniqueFunction) {
        return this.start().dedup(uniqueFunction);
    }

    public default GraphTraversal<A, A> except(final String sideEffectKey) {
        return this.start().except(sideEffectKey);
    }

    public default GraphTraversal<A, A> except(final Object exceptionObject) {
        return this.start().except((A) exceptionObject);
    }

    public default GraphTraversal<A, A> except(final Collection<A> exceptionCollection) {
        return this.start().except(exceptionCollection);
    }

    public default GraphTraversal<A, A> has(final String key) {
        return this.start().has(key);
    }

    public default GraphTraversal<A, A> has(final String key, final Object value) {
        return this.start().has(key, value);
    }

    public default GraphTraversal<A, A> has(final String key, final T t, final Object value) {
        return this.start().has(key, t, value);
    }

    public default GraphTraversal<A, A> has(final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(key, predicate, value);
    }

    public default GraphTraversal<A, A> has(final String label, final String key, final Object value) {
        return this.start().has(label, key, value);
    }

    public default GraphTraversal<A, A> has(final String label, final String key, final T t, final Object value) {
        return this.start().has(label, key, t, value);
    }

    public default GraphTraversal<A, A> has(final String label, final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(label, key, predicate, value);
    }

    public default GraphTraversal<A, A> hasNot(final String key) {
        return this.start().hasNot(key);
    }

    public default <E2> GraphTraversal<A, Map<String, E2>> where(final String firstKey, final String secondKey, final SBiPredicate predicate) {
        return this.start().where(firstKey, secondKey, predicate);
    }

    public default <E2> GraphTraversal<A, Map<String, E2>> where(final String firstKey, final SBiPredicate predicate, final String secondKey) {
        return this.start().where(firstKey, predicate, secondKey);
    }

    public default <E2> GraphTraversal<A, Map<String, E2>> where(final String firstKey, final T t, final String secondKey) {
        return this.start().where(firstKey, t, secondKey);
    }

    public default <E2> GraphTraversal<A, Map<String, E2>> where(final Traversal constraint) {
        return this.start().where(constraint);
    }

    public default GraphTraversal<A, A> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.start().interval(key, startValue, endValue);
    }

    public default GraphTraversal<A, A> random(final double probability) {
        return this.start().random(probability);
    }

    public default GraphTraversal<A, A> range(final int low, final int high) {
        return this.start().range(low, high);
    }

    public default GraphTraversal<A, A> retain(final String sideEffectKey) {
        return this.start().retain(sideEffectKey);
    }

    public default GraphTraversal<A, A> retain(final Object retainObject) {
        return this.start().retain((A) retainObject);
    }

    public default GraphTraversal<A, A> retain(final Collection<A> retainCollection) {
        return this.start().retain(retainCollection);
    }

    public default GraphTraversal<A, A> simplePath() {
        return this.start().simplePath();
    }

    public default GraphTraversal<A, A> cyclicPath() {
        return this.start().cyclicPath();
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default GraphTraversal<A, A> sideEffect(final SConsumer<Traverser<A>> consumer) {
        return this.start().sideEffect(consumer);
    }

    public default GraphTraversal<A, A> sideEffect(final SBiConsumer<Traverser<A>, Traversal.SideEffects> biConsumer) {
        return this.start().sideEffect(biConsumer);
    }

    public default <E2> GraphTraversal<A, E2> cap(final String sideEffectKey) {
        return this.start().cap(sideEffectKey);
    }

    public default <E2> GraphTraversal<A, E2> cap() {
        return this.start().cap();
    }

    public default GraphTraversal<A, A> subgraph(final String sideEffectKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, edgeIdHolder, vertexMap, includeEdge);
    }

    public default GraphTraversal<A, A> subgraph(final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, edgeIdHolder, vertexMap, includeEdge);
    }

    public default GraphTraversal<A, A> subgraph(final String sideEffectKey, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, null, null, includeEdge);
    }

    public default GraphTraversal<A, A> subgraph(final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, null, null, includeEdge);
    }

    public default GraphTraversal<A, A> aggregate(final String sideEffectKey, final SFunction<Traverser<A>, ?> preAggregateFunction) {
        return this.start().aggregate(sideEffectKey, preAggregateFunction);
    }

    public default GraphTraversal<A, A> aggregate(final SFunction<Traverser<A>, ?> preAggregateFunction) {
        return this.start().aggregate(null, preAggregateFunction);
    }

    public default GraphTraversal<A, A> aggregate() {
        return this.start().aggregate(null, null);
    }

    public default GraphTraversal<A, A> aggregate(final String sideEffectKey) {
        return this.start().aggregate(sideEffectKey, null);
    }

    public default GraphTraversal<A, A> groupBy(final String sideEffectKey, final SFunction<Traverser<A>, ?> keyFunction, final SFunction<Traverser<A>, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, reduceFunction);
    }


    public default GraphTraversal<A, A> groupBy(final SFunction<Traverser<A>, ?> keyFunction, final SFunction<Traverser<A>, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, reduceFunction);
    }

    public default GraphTraversal<A, A> groupBy(final SFunction<Traverser<A>, ?> keyFunction, final SFunction<Traverser<A>, ?> valueFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<A, A> groupBy(final SFunction<Traverser<A>, ?> keyFunction) {
        return this.start().groupBy(null, keyFunction, null, null);
    }

    public default GraphTraversal<A, A> groupBy(final String sideEffectKey, final SFunction<Traverser<A>, ?> keyFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, null, null);
    }

    public default GraphTraversal<A, A> groupBy(final String sideEffectKey, final SFunction<Traverser<A>, ?> keyFunction, final SFunction<Traverser<A>, ?> valueFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<A, A> groupCount(final String sideEffectKey, final SFunction<Traverser<A>, ?> preGroupFunction) {
        return this.start().groupCount(sideEffectKey, preGroupFunction);
    }

    public default GraphTraversal<A, A> groupCount(final SFunction<Traverser<A>, ?> preGroupFunction) {
        return this.start().groupCount(null, preGroupFunction);
    }

    public default GraphTraversal<A, A> groupCount(final String sideEffectKey) {
        return this.start().groupCount(sideEffectKey, null);
    }

    public default GraphTraversal<A, A> groupCount() {
        return this.start().groupCount(null, null);
    }

    public default GraphTraversal<A, Vertex> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addE(direction, edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<A, Vertex> addInE(final String edgeLabel, final String setLabel, final Object... propertyKeyValues) {
        return this.start().addInE(edgeLabel, setLabel, propertyKeyValues);
    }

    public default GraphTraversal<A, Vertex> addOutE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addOutE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<A, Vertex> addBothE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addBothE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<A, A> timeLimit(final long timeLimit) {
        return this.start().timeLimit(timeLimit);
    }

    public default GraphTraversal<A, A> tree(final String sideEffectKey, final SFunction... branchFunctions) {
        return this.start().tree(sideEffectKey, branchFunctions);
    }

    public default GraphTraversal<A, A> tree(final SFunction... branchFunctions) {
        return this.start().tree(null, branchFunctions);
    }

    public default GraphTraversal<A, A> store(final String sideEffectKey, final SFunction<Traverser<A>, ?> preStoreFunction) {
        return this.start().store(sideEffectKey, preStoreFunction);
    }

    public default GraphTraversal<A, A> store(final String sideEffectKey) {
        return this.start().store(sideEffectKey, null);
    }

    public default GraphTraversal<A, A> store(final SFunction<Traverser<A>, ?> preStoreFunction) {
        return this.start().store(null, preStoreFunction);
    }

    public default GraphTraversal<A, A> store() {
        return this.start().store(null, null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default GraphTraversal<A, A> jump(final String jumpLabel, final SPredicate<Traverser<A>> ifPredicate, final SPredicate<Traverser<A>> emitPredicate) {
        return this.start().jump(jumpLabel, ifPredicate, emitPredicate);
    }

    public default GraphTraversal<A, A> jump(final String jumpLabel, final SPredicate<Traverser<A>> ifPredicate) {
        return this.start().jump(jumpLabel, ifPredicate);
    }

    public default GraphTraversal<A, A> jump(final String jumpLabel, final int loops, final SPredicate<Traverser<A>> emitPredicate) {
        return this.start().jump(jumpLabel, loops, emitPredicate);
    }

    public default GraphTraversal<A, A> jump(final String jumpLabel, final int loops) {
        return this.start().jump(jumpLabel, loops);
    }

    public default GraphTraversal<A, A> jump(final String jumpLabel) {
        return this.start().jump(jumpLabel);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<A, A> as(final String label) {
        return this.start().as(label);
    }

    public default GraphTraversal<A, A> with(final Object... sideEffectKeyValues) {
        return this.start().with(sideEffectKeyValues);
    }
}
