package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
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

    public default <E2> GraphTraversal<Edge, E2> flatMap(final SFunction<Traverser<Edge>, Iterator<E2>> function) {
        return this.start().flatMap(function);
    }

    public default GraphTraversal<Edge, Edge> identity() {
        return this.start().identity();
    }

    public default GraphTraversal<Edge, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().to(direction, branchFactor, labels);
    }

    public default GraphTraversal<Edge, Vertex> to(final Direction direction, final String... labels) {
        return this.start().to(direction, labels);
    }

    public default GraphTraversal<Edge, Vertex> out(final int branchFactor, final String... labels) {
        return this.start().out(branchFactor, labels);
    }

    public default GraphTraversal<Edge, Vertex> out(final String... labels) {
        return this.start().out(labels);
    }

    public default GraphTraversal<Edge, Vertex> in(final int branchFactor, final String... labels) {
        return this.start().in(branchFactor, labels);
    }

    public default GraphTraversal<Edge, Vertex> in(final String... labels) {
        return this.start().in(labels);
    }

    public default GraphTraversal<Edge, Vertex> both(final int branchFactor, final String... labels) {
        return this.start().both(branchFactor, labels);
    }

    public default GraphTraversal<Edge, Vertex> both(final String... labels) {
        return this.start().both(labels);
    }

    public default GraphTraversal<Edge, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().toE(direction, branchFactor, labels);
    }

    public default GraphTraversal<Edge, Edge> toE(final Direction direction, final String... labels) {
        return this.start().toE(direction, labels);
    }

    public default GraphTraversal<Edge, Edge> outE(final int branchFactor, final String... labels) {
        return this.start().outE(branchFactor, labels);
    }

    public default GraphTraversal<Edge, Edge> outE(final String... labels) {
        return this.start().outE(labels);
    }

    public default GraphTraversal<Edge, Edge> inE(final int branchFactor, final String... labels) {
        return this.start().inE(branchFactor, labels);
    }

    public default GraphTraversal<Edge, Edge> inE(final String... labels) {
        return this.start().inE(labels);
    }

    public default GraphTraversal<Edge, Edge> bothE(final int branchFactor, final String... labels) {
        return this.start().bothE(branchFactor, labels);
    }

    public default GraphTraversal<Edge, Edge> bothE(final String... labels) {
        return this.start().bothE(labels);
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

    public default <E2> GraphTraversal<Edge, E2> back(final String as) {
        return this.start().back(as);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> match(final String inAs, final Traversal... traversals) {
        return this.start().match(inAs, traversals);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> select(final List<String> asLabels, SFunction... stepFunctions) {
        return this.start().select(asLabels, stepFunctions);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> select(final SFunction... stepFunctions) {
        return this.start().select(stepFunctions);
    }

    public default <E2> GraphTraversal<Edge, E2> select(final String as, SFunction stepFunction) {
        return this.start().select(as, stepFunction);
    }

    public default <E2> GraphTraversal<Edge, E2> select(final String as) {
        return this.start().select(as, null);
    }

    /*public default <E2> GraphTraversal<S, E2> union(final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new UnionStep(this, traversals));
    }*/

    /*public default <E2> GraphTraversal<S, E2> intersect(final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new IntersectStep(this, traversals));
    }*/

    public default <E2> GraphTraversal<Edge, E2> unfold() {
        return this.start().unfold();
    }

    public default GraphTraversal<Edge, List<Edge>> fold() {
        return this.start().fold();
    }

    public default <E2> GraphTraversal<Edge, E2> fold(final E2 seed, final SBiFunction<E2, Edge, E2> foldFunction) {
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

    public default GraphTraversal<Edge, Edge> inject(final Object... injections) {
        return this.start().inject((Edge[]) injections);
    }

    public default GraphTraversal<Edge, Edge> dedup() {
        return this.start().dedup();
    }

    public default GraphTraversal<Edge, Edge> dedup(final SFunction<Edge, ?> uniqueFunction) {
        return this.start().dedup(uniqueFunction);
    }

    public default GraphTraversal<Edge, Edge> except(final String memoryKey) {
        return this.start().except(memoryKey);
    }

    public default GraphTraversal<Edge, Edge> except(final Object exceptionObject) {
        return this.start().except((Edge) exceptionObject);
    }

    public default GraphTraversal<Edge, Edge> except(final Collection<Edge> exceptionCollection) {
        return this.start().except(exceptionCollection);
    }

    public default <E2> GraphTraversal<Edge, E2> has(final String key) {
        return this.start().has(key);
    }

    public default <E2> GraphTraversal<Edge, E2> has(final String key, final Object value) {
        return this.start().has(key, value);
    }

    public default <E2> GraphTraversal<Edge, E2> has(final String key, final T t, final Object value) {
        return this.start().has(key, t, value);
    }

    public default <E2> GraphTraversal<Edge, E2> has(final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(key, predicate, value);
    }

    public default <E2> GraphTraversal<Edge, E2> hasNot(final String key) {
        return this.start().hasNot(key);
    }

    public default <E2> GraphTraversal<Edge, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.start().interval(key, startValue, endValue);
    }

    public default GraphTraversal<Edge, Edge> random(final double probability) {
        return this.start().random(probability);
    }

    public default GraphTraversal<Edge, Edge> range(final int low, final int high) {
        return this.start().range(low, high);
    }

    public default GraphTraversal<Edge, Edge> retain(final String memoryKey) {
        return this.start().retain(memoryKey);
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

    public default <E2> GraphTraversal<Edge, E2> cap(final String memoryKey) {
        return this.start().cap(memoryKey);
    }

    public default <E2> GraphTraversal<Edge, E2> cap() {
        return this.start().cap();
    }

    public default GraphTraversal<Edge, Edge> subgraph(final String memoryKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(memoryKey, edgeIdHolder, vertexMap, includeEdge);
    }

    public default GraphTraversal<Edge, Edge> subgraph(final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, edgeIdHolder, vertexMap, includeEdge);
    }

    public default GraphTraversal<Edge, Edge> subgraph(final String memoryKey, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(memoryKey, null, null, includeEdge);
    }

    public default GraphTraversal<Edge, Edge> subgraph(final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, null, null, includeEdge);
    }

    public default GraphTraversal<Edge, Edge> aggregate(final String memoryKey, final SFunction<Edge, ?> preAggregateFunction) {
        return this.start().aggregate(memoryKey, preAggregateFunction);
    }

    public default GraphTraversal<Edge, Edge> aggregate(final SFunction<Edge, ?> preAggregateFunction) {
        return this.start().aggregate(null, preAggregateFunction);
    }

    public default GraphTraversal<Edge, Edge> aggregate() {
        return this.start().aggregate(null, null);
    }

    public default GraphTraversal<Edge, Edge> aggregate(final String memoryKey) {
        return this.start().aggregate(memoryKey, null);
    }

    public default GraphTraversal<Edge, Edge> groupBy(final String memoryKey, final SFunction<Edge, ?> keyFunction, final SFunction<Edge, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(memoryKey, keyFunction, valueFunction, reduceFunction);
    }


    public default GraphTraversal<Edge, Edge> groupBy(final SFunction<Edge, ?> keyFunction, final SFunction<Edge, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, reduceFunction);
    }

    public default GraphTraversal<Edge, Edge> groupBy(final SFunction<Edge, ?> keyFunction, final SFunction<Edge, ?> valueFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<Edge, Edge> groupBy(final SFunction<Edge, ?> keyFunction) {
        return this.start().groupBy(null, keyFunction, null, null);
    }

    public default GraphTraversal<Edge, Edge> groupBy(final String memoryKey, final SFunction<Edge, ?> keyFunction) {
        return this.start().groupBy(memoryKey, keyFunction, null, null);
    }

    public default GraphTraversal<Edge, Edge> groupBy(final String memoryKey, final SFunction<Edge, ?> keyFunction, final SFunction<Edge, ?> valueFunction) {
        return this.start().groupBy(memoryKey, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<Edge, Edge> groupCount(final String memoryKey, final SFunction<Edge, ?> preGroupFunction) {
        return this.start().groupCount(memoryKey, preGroupFunction);
    }

    public default GraphTraversal<Edge, Edge> groupCount(final SFunction<Edge, ?> preGroupFunction) {
        return this.start().groupCount(null, preGroupFunction);
    }

    public default GraphTraversal<Edge, Edge> groupCount(final String memoryKey) {
        return this.start().groupCount(memoryKey, null);
    }

    public default GraphTraversal<Edge, Edge> groupCount() {
        return this.start().groupCount(null, null);
    }

    public default GraphTraversal<Edge, Vertex> addE(final Direction direction, final String label, final String as, final Object... propertyKeyValues) {
        return this.start().addE(direction, label, as, propertyKeyValues);
    }

    public default GraphTraversal<Edge, Vertex> addInE(final String label, final String as, final Object... propertyKeyValues) {
        return this.start().addInE(label, as, propertyKeyValues);
    }

    public default GraphTraversal<Edge, Vertex> addOutE(final String label, final String as, final Object... propertyKeyValues) {
        return this.start().addOutE(label, as, propertyKeyValues);
    }

    public default GraphTraversal<Edge, Vertex> addBothE(final String label, final String as, final Object... propertyKeyValues) {
        return this.start().addBothE(label, as, propertyKeyValues);
    }

    public default GraphTraversal<Edge, Edge> timeLimit(final long timeLimit) {
        return this.start().timeLimit(timeLimit);
    }

    public default GraphTraversal<Edge, Edge> tree(final String memoryKey, final SFunction... branchFunctions) {
        return this.start().tree(memoryKey, branchFunctions);
    }

    public default GraphTraversal<Edge, Edge> tree(final SFunction... branchFunctions) {
        return this.start().tree(null, branchFunctions);
    }

    public default GraphTraversal<Edge, Edge> store(final String memoryKey, final SFunction<Edge, ?> preStoreFunction) {
        return this.start().store(memoryKey, preStoreFunction);
    }

    public default GraphTraversal<Edge, Edge> store(final String memoryKey) {
        return this.start().store(memoryKey, null);
    }

    public default GraphTraversal<Edge, Edge> store(final SFunction<Edge, ?> preStoreFunction) {
        return this.start().store(null, preStoreFunction);
    }

    public default GraphTraversal<Edge, Edge> store() {
        return this.start().store(null, null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default GraphTraversal<Edge, Edge> jump(final String as, final SPredicate<Traverser<Edge>> ifPredicate, final SPredicate<Traverser<Edge>> emitPredicate) {
        return this.start().jump(as, ifPredicate, emitPredicate);
    }

    public default GraphTraversal<Edge, Edge> jump(final String as, final SPredicate<Traverser<Edge>> ifPredicate) {
        return this.start().jump(as, ifPredicate);
    }

    public default GraphTraversal<Edge, Edge> jump(final String as, final int loops, final SPredicate<Traverser<Edge>> emitPredicate) {
        return this.start().jump(as, loops, emitPredicate);
    }

    public default GraphTraversal<Edge, Edge> jump(final String as, final int loops) {
        return this.start().jump(as, loops);
    }

    public default GraphTraversal<Edge, Edge> jump(final String as) {
        return this.start().jump(as);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<Edge, Edge> as(final String as) {
        return this.start().as(as);
    }

    public default GraphTraversal<Edge, Edge> with(final Object... memoryKeyValues) {
        return this.start().with(memoryKeyValues);
    }
}
