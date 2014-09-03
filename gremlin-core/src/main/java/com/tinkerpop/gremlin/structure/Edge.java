package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.util.function.SBiConsumer;
import com.tinkerpop.gremlin.util.function.SBiFunction;
import com.tinkerpop.gremlin.util.function.SBiPredicate;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * An {@link Edge} links two {@link Vertex} objects. Along with its {@link Property} objects, an {@link Edge} has both
 * a {@link Direction} and a {@code label}. The {@link Direction} determines which {@link Vertex} is the tail
 * {@link Vertex} (out {@link Vertex}) and which {@link Vertex} is the head {@link Vertex}
 * (in {@link Vertex}). The {@link Edge} {@code label} determines the type of relationship that exists between the
 * two vertices.
 * <p>
 * Diagrammatically:
 * <pre>
 * outVertex ---label---> inVertex.
 * </pre>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Edge extends Element {

    /**
     * The default label to use for an edge.
     * This is typically never used as when an edge is created, an edge label is required to be specified.
     */
    public static final String DEFAULT_LABEL = "edge";

    /**
     * Retrieve the vertex (or vertices) associated with this edge as defined by the direction.
     *
     * @param direction Get the incoming vertex, outgoing vertex, or both vertices
     * @return An iterator with 1 or 2 vertices
     */
    public Iterator<Vertex> vertices(final Direction direction);

    public <V> Iterator<Property<V>> properties(final String... propertyKeys);

    public <V> Iterator<Property<V>> hiddens(final String... propertyKeys);

    public default Map<String, Object> values() {
        return (Map) Element.super.values();
    }

    public default Map<String, Object> hiddenValues() {
       return (Map) Element.super.hiddenValues();
    }

    /**
     * Common exceptions to use with an edge.
     */
    public static class Exceptions extends Element.Exceptions {
        public static IllegalArgumentException edgeLabelCanNotBeNull() {
            return new IllegalArgumentException("Edge label can not be null");
        }

        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Edge does not support user supplied identifiers");
        }

        public static IllegalStateException edgeRemovalNotSupported() {
            return new IllegalStateException("Edge removal are not supported");
        }
    }

    public default GraphTraversal<Edge, Edge> start() {
        final GraphTraversal<Edge, Edge> traversal = GraphTraversal.of();
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    //////////////////////////////////////////////////////////////////////

    public default GraphTraversal<Edge, Edge> trackPaths() {
        return this.start().trackPaths();
    }

    public default GraphTraversal<Edge, Long> count() {
        return this.start().count();
    }

    public default GraphTraversal<Edge, Edge> submit(final GraphComputer graphComputer) {
        return this.start().submit(graphComputer);
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> GraphTraversal<Edge, E2> map(final SFunction<Traverser<Edge>, E2> function) {
        return this.start().map(function);
    }

    public default <E2> GraphTraversal<Edge, E2> map(final SBiFunction<Traverser<Edge>, Traversal.SideEffects, E2> biFunction) {
        return this.start().map(biFunction);
    }

    public default <E2> GraphTraversal<Edge, E2> flatMap(final SFunction<Traverser<Edge>, Iterator<E2>> function) {
        return this.start().flatMap(function);
    }

    public default <E2> GraphTraversal<Edge, E2> flatMap(final SBiFunction<Traverser<Edge>, Traversal.SideEffects, Iterator<E2>> biFunction) {
        return this.start().flatMap(biFunction);
    }

    public default GraphTraversal<Edge, Edge> identity() {
        return this.start().identity();
    }

    public default GraphTraversal<Edge, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().to(direction, branchFactor, labels);
    }

    public default GraphTraversal<Edge, Vertex> to(final Direction direction, final String... edgeLabels) {
        return this.start().to(direction, edgeLabels);
    }

    public default GraphTraversal<Edge, Vertex> out(final int branchFactor, final String... edgeLabels) {
        return this.start().out(branchFactor, edgeLabels);
    }

    public default GraphTraversal<Edge, Vertex> out(final String... edgeLabels) {
        return this.start().out(edgeLabels);
    }

    public default GraphTraversal<Edge, Vertex> in(final int branchFactor, final String... edgeLabels) {
        return this.start().in(branchFactor, edgeLabels);
    }

    public default GraphTraversal<Edge, Vertex> in(final String... edgeLabels) {
        return this.start().in(edgeLabels);
    }

    public default GraphTraversal<Edge, Vertex> both(final int branchFactor, final String... edgeLabels) {
        return this.start().both(branchFactor, edgeLabels);
    }

    public default GraphTraversal<Edge, Vertex> both(final String... edgeLabels) {
        return this.start().both(edgeLabels);
    }

    public default GraphTraversal<Edge, Edge> toE(final Direction direction, final int branchFactor, final String... edgeLabels) {
        return this.start().toE(direction, branchFactor, edgeLabels);
    }

    public default GraphTraversal<Edge, Edge> toE(final Direction direction, final String... edgeLabels) {
        return this.start().toE(direction, edgeLabels);
    }

    public default GraphTraversal<Edge, Edge> outE(final int branchFactor, final String... edgeLabels) {
        return this.start().outE(branchFactor, edgeLabels);
    }

    public default GraphTraversal<Edge, Edge> outE(final String... edgeLabels) {
        return this.start().outE(edgeLabels);
    }

    public default GraphTraversal<Edge, Edge> inE(final int branchFactor, final String... edgeLabels) {
        return this.start().inE(branchFactor, edgeLabels);
    }

    public default GraphTraversal<Edge, Edge> inE(final String... edgeLabels) {
        return this.start().inE(edgeLabels);
    }

    public default GraphTraversal<Edge, Edge> bothE(final int branchFactor, final String... edgeLabels) {
        return this.start().bothE(branchFactor, edgeLabels);
    }

    public default GraphTraversal<Edge, Edge> bothE(final String... edgeLabels) {
        return this.start().bothE(edgeLabels);
    }

    public default GraphTraversal<Edge, Vertex> toV(final Direction direction) {
        return this.start().toV(direction);
    }

    public default GraphTraversal<Edge, Vertex> inV() {
        return this.start().inV();
    }

    public default GraphTraversal<Edge, Vertex> outV() {
        return this.start().outV();
    }

    public default GraphTraversal<Edge, Vertex> bothV() {
        return this.start().bothV();
    }

    public default GraphTraversal<Edge, Vertex> otherV() {
        return this.start().otherV();
    }

    public default GraphTraversal<Edge, Edge> order() {
        return this.start().order();
    }

    public default GraphTraversal<Edge, Edge> order(final Comparator<Traverser<Edge>> comparator) {
        return this.start().order(comparator);
    }

    public default GraphTraversal<Edge, Edge> shuffle() {
        return this.start().shuffle();
    }

    public default <E2> GraphTraversal<Edge, E2> value() {
        return this.start().value();
    }

    public default <E2> GraphTraversal<Edge, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.start().value(propertyKey, defaultSupplier);
    }

    public default GraphTraversal<Edge, Map<String, Object>> values(final String... propertyKeys) {
        return this.start().values(propertyKeys);
    }

    public default GraphTraversal<Edge, Path> path(final SFunction... pathFunctions) {
        return this.start().path(pathFunctions);
    }

    public default <E2> GraphTraversal<Edge, E2> back(final String stepLabel) {
        return this.start().back(stepLabel);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> match(final String startLabel, final Traversal... traversals) {
        return this.start().match(startLabel, traversals);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> select(final List<String> labels, SFunction... stepFunctions) {
        return this.start().select(labels, stepFunctions);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> select(final SFunction... stepFunctions) {
        return this.start().select(stepFunctions);
    }

    public default <E2> GraphTraversal<Edge, E2> select(final String label, SFunction stepFunction) {
        return this.start().select(label, stepFunction);
    }

    public default <E2> GraphTraversal<Edge, E2> select(final String label) {
        return this.start().select(label, null);
    }

    /*public default <E2> GraphTraversal<S, E2> union(final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new UnionStep(this, traversals));
    }*/

    /*public default <E2> GraphTraversal<S, E2> intersect(final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new IntersectStep(this, traversals));
    }*/

    public default GraphTraversal<Edge, Edge> unfold() {
        return this.start().unfold();
    }

    public default GraphTraversal<Edge, List<Edge>> fold() {
        return this.start().fold();
    }

    public default <E2> GraphTraversal<Edge, E2> fold(final E2 seed, final SBiFunction<E2, Traverser<Edge>, E2> foldFunction) {
        return this.start().fold(seed, foldFunction);
    }

    public default <E2> GraphTraversal<Edge, E2> choose(final SPredicate<Traverser<Edge>> choosePredicate, final Traversal trueChoice, final Traversal falseChoice) {
        return this.start().choose(choosePredicate, trueChoice, falseChoice);
    }

    public default <E2, M> GraphTraversal<Edge, E2> choose(final SFunction<Traverser<Edge>, M> mapFunction, final Map<M, Traversal<Edge, E2>> choices) {
        return this.start().choose(mapFunction, choices);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default GraphTraversal<Edge, Edge> filter(final SPredicate<Traverser<Edge>> predicate) {
        return this.start().filter(predicate);
    }

    public default GraphTraversal<Edge, Edge> filter(final SBiPredicate<Traverser<Edge>, Traversal.SideEffects> biPredicate) {
        return this.start().filter(biPredicate);
    }

    public default GraphTraversal<Edge, Edge> inject(final Object... injections) {
        return this.start().inject((Edge[]) injections);
    }

    public default GraphTraversal<Edge, Edge> dedup() {
        return this.start().dedup();
    }

    public default GraphTraversal<Edge, Edge> dedup(final SFunction<Traverser<Edge>, ?> uniqueFunction) {
        return this.start().dedup(uniqueFunction);
    }

    public default GraphTraversal<Edge, Edge> except(final String sideEffectKey) {
        return this.start().except(sideEffectKey);
    }

    public default GraphTraversal<Edge, Edge> except(final Object exceptionObject) {
        return this.start().except((Edge) exceptionObject);
    }

    public default GraphTraversal<Edge, Edge> except(final Collection<Edge> exceptionCollection) {
        return this.start().except(exceptionCollection);
    }

    public default GraphTraversal<Edge, Edge> has(final String key) {
        return this.start().has(key);
    }

    public default GraphTraversal<Edge, Edge> has(final String key, final Object value) {
        return this.start().has(key, value);
    }

    public default GraphTraversal<Edge, Edge> has(final String key, final T t, final Object value) {
        return this.start().has(key, t, value);
    }

    public default GraphTraversal<Edge, Edge> has(final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(key, predicate, value);
    }

    public default GraphTraversal<Edge, Edge> has(final String label, final String key, final Object value) {
        return this.start().has(label, key, value);
    }

    public default GraphTraversal<Edge, Edge> has(final String label, final String key, final T t, final Object value) {
        return this.start().has(label, key, t, value);
    }

    public default GraphTraversal<Edge, Edge> has(final String label, final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(label, key, predicate, value);
    }

    public default GraphTraversal<Edge, Edge> hasNot(final String key) {
        return this.start().hasNot(key);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> where(final String firstKey, final String secondKey, final SBiPredicate predicate) {
        return this.start().where(firstKey, secondKey, predicate);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> where(final String firstKey, final SBiPredicate predicate, final String secondKey) {
        return this.start().where(firstKey, predicate, secondKey);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> where(final String firstKey, final T t, final String secondKey) {
        return this.start().where(firstKey, t, secondKey);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> where(final Traversal constraint) {
        return this.start().where(constraint);
    }

    public default GraphTraversal<Edge, Edge> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.start().interval(key, startValue, endValue);
    }

    public default GraphTraversal<Edge, Edge> random(final double probability) {
        return this.start().random(probability);
    }

    public default GraphTraversal<Edge, Edge> range(final int low, final int high) {
        return this.start().range(low, high);
    }

    public default GraphTraversal<Edge, Edge> retain(final String sideEffectKey) {
        return this.start().retain(sideEffectKey);
    }

    public default GraphTraversal<Edge, Edge> retain(final Object retainObject) {
        return this.start().retain((Edge) retainObject);
    }

    public default GraphTraversal<Edge, Edge> retain(final Collection<Edge> retainCollection) {
        return this.start().retain(retainCollection);
    }

    public default GraphTraversal<Edge, Edge> simplePath() {
        return this.start().simplePath();
    }

    public default GraphTraversal<Edge, Edge> cyclicPath() {
        return this.start().cyclicPath();
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default GraphTraversal<Edge, Edge> sideEffect(final SConsumer<Traverser<Edge>> consumer) {
        return this.start().sideEffect(consumer);
    }

    public default GraphTraversal<Edge, Edge> sideEffect(final SBiConsumer<Traverser<Edge>, Traversal.SideEffects> biConsumer) {
        return this.start().sideEffect(biConsumer);
    }

    public default <E2> GraphTraversal<Edge, E2> cap(final String sideEffectKey) {
        return this.start().cap(sideEffectKey);
    }

    public default <E2> GraphTraversal<Edge, E2> cap() {
        return this.start().cap();
    }

    public default GraphTraversal<Edge, Edge> subgraph(final String sideEffectKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, edgeIdHolder, vertexMap, includeEdge);
    }

    public default GraphTraversal<Edge, Edge> subgraph(final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, edgeIdHolder, vertexMap, includeEdge);
    }

    public default GraphTraversal<Edge, Edge> subgraph(final String sideEffectKey, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(sideEffectKey, null, null, includeEdge);
    }

    public default GraphTraversal<Edge, Edge> subgraph(final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, null, null, includeEdge);
    }

    public default GraphTraversal<Edge, Edge> aggregate(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> preAggregateFunction) {
        return this.start().aggregate(sideEffectKey, preAggregateFunction);
    }

    public default GraphTraversal<Edge, Edge> aggregate(final SFunction<Traverser<Edge>, ?> preAggregateFunction) {
        return this.start().aggregate(null, preAggregateFunction);
    }

    public default GraphTraversal<Edge, Edge> aggregate() {
        return this.start().aggregate(null, null);
    }

    public default GraphTraversal<Edge, Edge> aggregate(final String sideEffectKey) {
        return this.start().aggregate(sideEffectKey, null);
    }

    public default GraphTraversal<Edge, Edge> groupBy(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, reduceFunction);
    }


    public default GraphTraversal<Edge, Edge> groupBy(final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, reduceFunction);
    }

    public default GraphTraversal<Edge, Edge> groupBy(final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<Edge, Edge> groupBy(final SFunction<Traverser<Edge>, ?> keyFunction) {
        return this.start().groupBy(null, keyFunction, null, null);
    }

    public default GraphTraversal<Edge, Edge> groupBy(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> keyFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, null, null);
    }

    public default GraphTraversal<Edge, Edge> groupBy(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> keyFunction, final SFunction<Traverser<Edge>, ?> valueFunction) {
        return this.start().groupBy(sideEffectKey, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<Edge, Edge> groupCount(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> preGroupFunction) {
        return this.start().groupCount(sideEffectKey, preGroupFunction);
    }

    public default GraphTraversal<Edge, Edge> groupCount(final SFunction<Traverser<Edge>, ?> preGroupFunction) {
        return this.start().groupCount(null, preGroupFunction);
    }

    public default GraphTraversal<Edge, Edge> groupCount(final String sideEffectKey) {
        return this.start().groupCount(sideEffectKey, null);
    }

    public default GraphTraversal<Edge, Edge> groupCount() {
        return this.start().groupCount(null, null);
    }

    public default GraphTraversal<Edge, Vertex> addE(final Direction direction, final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addE(direction, edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<Edge, Vertex> addInE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addInE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<Edge, Vertex> addOutE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addOutE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<Edge, Vertex> addBothE(final String edgeLabel, final String stepLabel, final Object... propertyKeyValues) {
        return this.start().addBothE(edgeLabel, stepLabel, propertyKeyValues);
    }

    public default GraphTraversal<Edge, Edge> timeLimit(final long timeLimit) {
        return this.start().timeLimit(timeLimit);
    }

    public default GraphTraversal<Edge, Edge> tree(final String sideEffectKey, final SFunction... branchFunctions) {
        return this.start().tree(sideEffectKey, branchFunctions);
    }

    public default GraphTraversal<Edge, Edge> tree(final SFunction... branchFunctions) {
        return this.start().tree(null, branchFunctions);
    }

    public default GraphTraversal<Edge, Edge> store(final String sideEffectKey, final SFunction<Traverser<Edge>, ?> preStoreFunction) {
        return this.start().store(sideEffectKey, preStoreFunction);
    }

    public default GraphTraversal<Edge, Edge> store(final String sideEffectKey) {
        return this.start().store(sideEffectKey, null);
    }

    public default GraphTraversal<Edge, Edge> store(final SFunction<Traverser<Edge>, ?> preStoreFunction) {
        return this.start().store(null, preStoreFunction);
    }

    public default GraphTraversal<Edge, Edge> store() {
        return this.start().store(null, null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default GraphTraversal<Edge, Edge> jump(final String jumpLabel, final SPredicate<Traverser<Edge>> ifPredicate, final SPredicate<Traverser<Edge>> emitPredicate) {
        return this.start().jump(jumpLabel, ifPredicate, emitPredicate);
    }

    public default GraphTraversal<Edge, Edge> jump(final String jumpLabel, final SPredicate<Traverser<Edge>> ifPredicate) {
        return this.start().jump(jumpLabel, ifPredicate);
    }

    public default GraphTraversal<Edge, Edge> jump(final String jumpLabel, final int loops, final SPredicate<Traverser<Edge>> emitPredicate) {
        return this.start().jump(jumpLabel, loops, emitPredicate);
    }

    public default GraphTraversal<Edge, Edge> jump(final String jumpLabel, final int loops) {
        return this.start().jump(jumpLabel, loops);
    }

    public default GraphTraversal<Edge, Edge> jump(final String jumpLabel) {
        return this.start().jump(jumpLabel);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<Edge, Edge> as(final String label) {
        return this.start().as(label);
    }

    public default GraphTraversal<Edge, Edge> with(final Object... sideEffectKeyValues) {
        return this.start().with(sideEffectKeyValues);
    }
}
