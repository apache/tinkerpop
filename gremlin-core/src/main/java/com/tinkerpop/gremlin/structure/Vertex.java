package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
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
 * A {@link Vertex} maintains pointers to both a set of incoming and outgoing {@link Edge} objects. The outgoing edges
 * are those edges for  which the {@link Vertex} is the tail. The incoming edges are those edges for which the
 * {@link Vertex} is the head.
 * <p>
 * Diagrammatically:
 * <pre>
 * ---inEdges---> vertex ---outEdges--->.
 * </pre>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Vertex extends Element {

    /**
     * The default label to use for a vertex.
     */
    public static final String DEFAULT_LABEL = "vertex";

    /**
     * Add an outgoing edge to the vertex with provided label and edge properties as key/value pairs.
     * These key/values must be provided in an even number where the odd numbered arguments are {@link String}
     * property keys and the even numbered arguments are the related property values.  Hidden properties can be
     * set by specifying the key as {@link com.tinkerpop.gremlin.structure.Graph.Key#hide}.
     *
     * @param label     The label of the edge
     * @param inVertex  The vertex to receive an incoming edge from the current vertex
     * @param keyValues The key/value pairs to turn into edge properties
     * @return the newly created edge
     */
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues);

    /**
     * The following iterators need to be implemented by the vendor as these define how edges are
     * retrieved off of a vertex. All other steps are derivatives of this and thus, defaulted in Vertex.
     */

    /**
     * @param direction    The incident direction of the edges to retrieve off this vertex
     * @param branchFactor The max number of edges to retrieve
     * @param labels       The labels of the edges to retrieve
     * @return An iterator of edges meeting the provided specification
     */
    public Iterator<Edge> edges(final Direction direction, final int branchFactor, final String... labels);

    /**
     * @param direction    The adjacency direction of the vertices to retrieve off this vertex
     * @param branchFactor The max number of vertices to retrieve
     * @param labels       The labels of the edges associated with the vertices to retrieve
     * @return An iterator of vertices meeting the provided specification
     */
    public Iterator<Vertex> vertices(final Direction direction, final int branchFactor, final String... labels);

    /**
     * Common exceptions to use with a vertex.
     */
    public static class Exceptions {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }

        public static IllegalStateException vertexRemovalNotSupported() {
            return new IllegalStateException("Vertex removal are not supported");
        }

        public static IllegalStateException edgeAdditionsNotSupported() {
            return new IllegalStateException("Edge additions not supported");
        }
    }

    public default GraphTraversal<Vertex, Vertex> start() {
        final GraphTraversal<Vertex, Vertex> traversal = GraphTraversal.of();
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    //////////////////////////////////////////////////////////////////////

    public default GraphTraversal<Vertex, Vertex> trackPaths() {
        return this.start().trackPaths();
    }

    public default GraphTraversal<Vertex, Long> count() {
        return this.start().count();
    }

    public default GraphTraversal<Vertex, Vertex> submit(final GraphComputer graphComputer) {
        return this.start().submit(graphComputer);
    }

    ///////////////////// TRANSFORM STEPS /////////////////////

    public default <E2> GraphTraversal<Vertex, E2> map(final SFunction<Traverser<Vertex>, E2> function) {
        return this.start().map(function);
    }

    public default <E2> GraphTraversal<Vertex, E2> flatMap(final SFunction<Traverser<Vertex>, Iterator<E2>> function) {
        return this.start().flatMap(function);
    }

    public default GraphTraversal<Vertex, Vertex> identity() {
        return this.start().identity();
    }

    public default GraphTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().to(direction, branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Vertex> to(final Direction direction, final String... labels) {
        return this.start().to(direction, labels);
    }

    public default GraphTraversal<Vertex, Vertex> out(final int branchFactor, final String... labels) {
        return this.start().out(branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Vertex> out(final String... labels) {
        return this.start().out(labels);
    }

    public default GraphTraversal<Vertex, Vertex> in(final int branchFactor, final String... labels) {
        return this.start().in(branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Vertex> in(final String... labels) {
        return this.start().in(labels);
    }

    public default GraphTraversal<Vertex, Vertex> both(final int branchFactor, final String... labels) {
        return this.start().both(branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Vertex> both(final String... labels) {
        return this.start().both(labels);
    }

    public default GraphTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().toE(direction, branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Edge> toE(final Direction direction, final String... labels) {
        return this.start().toE(direction, labels);
    }

    public default GraphTraversal<Vertex, Edge> outE(final int branchFactor, final String... labels) {
        return this.start().outE(branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Edge> outE(final String... labels) {
        return this.start().outE(labels);
    }

    public default GraphTraversal<Vertex, Edge> inE(final int branchFactor, final String... labels) {
        return this.start().inE(branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Edge> inE(final String... labels) {
        return this.start().inE(labels);
    }

    public default GraphTraversal<Vertex, Edge> bothE(final int branchFactor, final String... labels) {
        return this.start().bothE(branchFactor, labels);
    }

    public default GraphTraversal<Vertex, Edge> bothE(final String... labels) {
        return this.start().bothE(labels);
    }

    public default GraphTraversal<Vertex, Vertex> toV(final Direction direction) {
        return this.start().toV(direction);
    }

    public default GraphTraversal<Vertex, Vertex> inV() {
        return this.start().inV();
    }

    public default GraphTraversal<Vertex, Vertex> outV() {
        return this.start().outV();
    }

    public default GraphTraversal<Vertex, Vertex> bothV() {
        return this.start().bothV();
    }

    public default GraphTraversal<Vertex, Vertex> otherV() {
        return this.start().otherV();
    }

    public default GraphTraversal<Vertex, Vertex> order() {
        return this.start().order();
    }

    public default GraphTraversal<Vertex, Vertex> order(final Comparator<Traverser<Vertex>> comparator) {
        return this.start().order(comparator);
    }

    public default GraphTraversal<Vertex, Vertex> shuffle() {
        return this.start().shuffle();
    }

    public default <E2> GraphTraversal<Vertex, E2> value() {
        return this.start().value();
    }

    public default <E2> GraphTraversal<Vertex, E2> value(final String propertyKey, final Supplier<E2> defaultSupplier) {
        return this.start().value(propertyKey, defaultSupplier);
    }

    public default GraphTraversal<Vertex, Map<String, Object>> values(final String... propertyKeys) {
        return this.start().values(propertyKeys);
    }

    public default GraphTraversal<Vertex, Path> path(final SFunction... pathFunctions) {
        return this.start().path(pathFunctions);
    }

    public default <E2> GraphTraversal<Vertex, E2> back(final String as) {
        return this.start().back(as);
    }

    public default <E2> GraphTraversal<Vertex, Map<String, E2>> match(final String inAs, final Traversal... traversals) {
        return this.start().match(inAs, traversals);
    }

    public default <E2> GraphTraversal<Vertex, Map<String, E2>> select(final List<String> asLabels, SFunction... stepFunctions) {
        return this.start().select(asLabels, stepFunctions);
    }

    public default <E2> GraphTraversal<Vertex, Map<String, E2>> select(final SFunction... stepFunctions) {
        return this.start().select(stepFunctions);
    }

    public default <E2> GraphTraversal<Vertex, E2> select(final String as, SFunction stepFunction) {
        return this.start().select(as, stepFunction);
    }

    public default <E2> GraphTraversal<Vertex, E2> select(final String as) {
        return this.start().select(as, null);
    }

    /*public default <E2> GraphTraversal<S, E2> union(final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new UnionStep(this, traversals));
    }*/

    /*public default <E2> GraphTraversal<S, E2> intersect(final Traversal... traversals) {
        return (GraphTraversal) this.addStep(new IntersectStep(this, traversals));
    }*/

    public default <E2> GraphTraversal<Vertex, E2> unfold() {
        return this.start().unfold();
    }

    public default GraphTraversal<Vertex, List<Vertex>> fold() {
        return this.start().fold();
    }

    public default <E2> GraphTraversal<Vertex, E2> fold(final E2 seed, final SBiFunction<E2, Vertex, E2> foldFunction) {
        return this.start().fold(seed, foldFunction);
    }

    public default <E2> GraphTraversal<Vertex, E2> choose(final SPredicate<Traverser<Vertex>> choosePredicate, final Traversal trueChoice, final Traversal falseChoice) {
        return this.start().choose(choosePredicate, trueChoice, falseChoice);
    }

    public default <E2, M> GraphTraversal<Vertex, E2> choose(final SFunction<Traverser<Vertex>, M> mapFunction, final Map<M, Traversal<Vertex, E2>> choices) {
        return this.start().choose(mapFunction, choices);
    }

    ///////////////////// FILTER STEPS /////////////////////

    public default GraphTraversal<Vertex, Vertex> filter(final SPredicate<Traverser<Vertex>> predicate) {
        return this.start().filter(predicate);
    }

    public default GraphTraversal<Vertex, Vertex> inject(final Object... injections) {
        return this.start().inject((Vertex[]) injections);
    }

    public default GraphTraversal<Vertex, Vertex> dedup() {
        return this.start().dedup();
    }

    public default GraphTraversal<Vertex, Vertex> dedup(final SFunction<Vertex, ?> uniqueFunction) {
        return this.start().dedup(uniqueFunction);
    }

    public default GraphTraversal<Vertex, Vertex> except(final String memoryKey) {
        return this.start().except(memoryKey);
    }

    public default GraphTraversal<Vertex, Vertex> except(final Object exceptionObject) {
        return this.start().except((Vertex) exceptionObject);
    }

    public default GraphTraversal<Vertex, Vertex> except(final Collection<Vertex> exceptionCollection) {
        return this.start().except(exceptionCollection);
    }

    public default <E2> GraphTraversal<Vertex, E2> has(final String key) {
        return this.start().has(key);
    }

    public default <E2> GraphTraversal<Vertex, E2> has(final String key, final Object value) {
        return this.start().has(key, value);
    }

    public default <E2> GraphTraversal<Vertex, E2> has(final String key, final T t, final Object value) {
        return this.start().has(key, t, value);
    }

    public default <E2> GraphTraversal<Vertex, E2> has(final String key, final SBiPredicate predicate, final Object value) {
        return this.start().has(key, predicate, value);
    }

    public default <E2> GraphTraversal<Vertex, E2> hasNot(final String key) {
        return this.start().hasNot(key);
    }

    public default GraphTraversal<Vertex, Map<String, Object>> given(final String firstKey, final String secondKey, final SBiPredicate predicate) {
        return this.start().given(firstKey, secondKey, predicate);
    }

    public default GraphTraversal<Vertex, Map<String, Object>> given(final String firstKey, final SBiPredicate predicate, final String secondKey) {
        return this.start().given(firstKey, predicate, secondKey);
    }

    public default GraphTraversal<Vertex, Map<String, Object>> given(final String firstKey, final T t, final String secondKey) {
        return this.start().given(firstKey, t, secondKey);
    }

    public default <E2> GraphTraversal<Vertex, E2> interval(final String key, final Comparable startValue, final Comparable endValue) {
        return this.start().interval(key, startValue, endValue);
    }

    public default GraphTraversal<Vertex, Vertex> random(final double probability) {
        return this.start().random(probability);
    }

    public default GraphTraversal<Vertex, Vertex> range(final int low, final int high) {
        return this.start().range(low, high);
    }

    public default GraphTraversal<Vertex, Vertex> retain(final String memoryKey) {
        return this.start().retain(memoryKey);
    }

    public default GraphTraversal<Vertex, Vertex> retain(final Object retainObject) {
        return this.start().retain((Vertex) retainObject);
    }

    public default GraphTraversal<Vertex, Vertex> retain(final Collection<Vertex> retainCollection) {
        return this.start().retain(retainCollection);
    }

    public default GraphTraversal<Vertex, Vertex> simplePath() {
        return this.start().simplePath();
    }

    public default GraphTraversal<Vertex, Vertex> cyclicPath() {
        return this.start().cyclicPath();
    }

    ///////////////////// SIDE-EFFECT STEPS /////////////////////

    public default GraphTraversal<Vertex, Vertex> sideEffect(final SConsumer<Traverser<Vertex>> consumer) {
        return this.start().sideEffect(consumer);
    }

    public default <E2> GraphTraversal<Vertex, E2> cap(final String memoryKey) {
        return this.start().cap(memoryKey);
    }

    public default <E2> GraphTraversal<Vertex, E2> cap() {
        return this.start().cap();
    }

    public default GraphTraversal<Vertex, Vertex> subgraph(final String memoryKey, final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(memoryKey, edgeIdHolder, vertexMap, includeEdge);
    }

    public default GraphTraversal<Vertex, Vertex> subgraph(final Set<Object> edgeIdHolder, final Map<Object, Vertex> vertexMap, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, edgeIdHolder, vertexMap, includeEdge);
    }

    public default GraphTraversal<Vertex, Vertex> subgraph(final String memoryKey, final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(memoryKey, null, null, includeEdge);
    }

    public default GraphTraversal<Vertex, Vertex> subgraph(final SPredicate<Edge> includeEdge) {
        return this.start().subgraph(null, null, null, includeEdge);
    }

    public default GraphTraversal<Vertex, Vertex> aggregate(final String memoryKey, final SFunction<Vertex, ?> preAggregateFunction) {
        return this.start().aggregate(memoryKey, preAggregateFunction);
    }

    public default GraphTraversal<Vertex, Vertex> aggregate(final SFunction<Vertex, ?> preAggregateFunction) {
        return this.start().aggregate(null, preAggregateFunction);
    }

    public default GraphTraversal<Vertex, Vertex> aggregate() {
        return this.start().aggregate(null, null);
    }

    public default GraphTraversal<Vertex, Vertex> aggregate(final String memoryKey) {
        return this.start().aggregate(memoryKey, null);
    }

    public default GraphTraversal<Vertex, Vertex> groupBy(final String memoryKey, final SFunction<Vertex, ?> keyFunction, final SFunction<Vertex, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(memoryKey, keyFunction, valueFunction, reduceFunction);
    }


    public default GraphTraversal<Vertex, Vertex> groupBy(final SFunction<Vertex, ?> keyFunction, final SFunction<Vertex, ?> valueFunction, final SFunction<Collection, ?> reduceFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, reduceFunction);
    }

    public default GraphTraversal<Vertex, Vertex> groupBy(final SFunction<Vertex, ?> keyFunction, final SFunction<Vertex, ?> valueFunction) {
        return this.start().groupBy(null, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<Vertex, Vertex> groupBy(final SFunction<Vertex, ?> keyFunction) {
        return this.start().groupBy(null, keyFunction, null, null);
    }

    public default GraphTraversal<Vertex, Vertex> groupBy(final String memoryKey, final SFunction<Vertex, ?> keyFunction) {
        return this.start().groupBy(memoryKey, keyFunction, null, null);
    }

    public default GraphTraversal<Vertex, Vertex> groupBy(final String memoryKey, final SFunction<Vertex, ?> keyFunction, final SFunction<Vertex, ?> valueFunction) {
        return this.start().groupBy(memoryKey, keyFunction, valueFunction, null);
    }

    public default GraphTraversal<Vertex, Vertex> groupCount(final String memoryKey, final SFunction<Vertex, ?> preGroupFunction) {
        return this.start().groupCount(memoryKey, preGroupFunction);
    }

    public default GraphTraversal<Vertex, Vertex> groupCount(final SFunction<Vertex, ?> preGroupFunction) {
        return this.start().groupCount(null, preGroupFunction);
    }

    public default GraphTraversal<Vertex, Vertex> groupCount(final String memoryKey) {
        return this.start().groupCount(memoryKey, null);
    }

    public default GraphTraversal<Vertex, Vertex> groupCount() {
        return this.start().groupCount(null, null);
    }

    public default GraphTraversal<Vertex, Vertex> addE(final Direction direction, final String label, final String as, final Object... propertyKeyValues) {
        return this.start().addE(direction, label, as, propertyKeyValues);
    }

    public default GraphTraversal<Vertex, Vertex> addInE(final String label, final String as, final Object... propertyKeyValues) {
        return this.start().addInE(label, as, propertyKeyValues);
    }

    public default GraphTraversal<Vertex, Vertex> addOutE(final String label, final String as, final Object... propertyKeyValues) {
        return this.start().addOutE(label, as, propertyKeyValues);
    }

    public default GraphTraversal<Vertex, Vertex> addBothE(final String label, final String as, final Object... propertyKeyValues) {
        return this.start().addBothE(label, as, propertyKeyValues);
    }

    public default GraphTraversal<Vertex, Vertex> timeLimit(final long timeLimit) {
        return this.start().timeLimit(timeLimit);
    }

    public default GraphTraversal<Vertex, Vertex> tree(final String memoryKey, final SFunction... branchFunctions) {
        return this.start().tree(memoryKey, branchFunctions);
    }

    public default GraphTraversal<Vertex, Vertex> tree(final SFunction... branchFunctions) {
        return this.start().tree(null, branchFunctions);
    }

    public default GraphTraversal<Vertex, Vertex> store(final String memoryKey, final SFunction<Vertex, ?> preStoreFunction) {
        return this.start().store(memoryKey, preStoreFunction);
    }

    public default GraphTraversal<Vertex, Vertex> store(final String memoryKey) {
        return this.start().store(memoryKey, null);
    }

    public default GraphTraversal<Vertex, Vertex> store(final SFunction<Vertex, ?> preStoreFunction) {
        return this.start().store(null, preStoreFunction);
    }

    public default GraphTraversal<Vertex, Vertex> store() {
        return this.start().store(null, null);
    }

    ///////////////////// BRANCH STEPS /////////////////////

    public default GraphTraversal<Vertex, Vertex> jump(final String as, final SPredicate<Traverser<Vertex>> ifPredicate, final SPredicate<Traverser<Vertex>> emitPredicate) {
        return this.start().jump(as, ifPredicate, emitPredicate);
    }

    public default GraphTraversal<Vertex, Vertex> jump(final String as, final SPredicate<Traverser<Vertex>> ifPredicate) {
        return this.start().jump(as, ifPredicate);
    }

    public default GraphTraversal<Vertex, Vertex> jump(final String as, final int loops, final SPredicate<Traverser<Vertex>> emitPredicate) {
        return this.start().jump(as, loops, emitPredicate);
    }

    public default GraphTraversal<Vertex, Vertex> jump(final String as, final int loops) {
        return this.start().jump(as, loops);
    }

    public default GraphTraversal<Vertex, Vertex> jump(final String as) {
        return this.start().jump(as);
    }

    ///////////////////// UTILITY STEPS /////////////////////

    public default GraphTraversal<Vertex, Vertex> as(final String as) {
        return this.start().as(as);
    }

    public default GraphTraversal<Vertex, Vertex> with(final Object... memoryKeyValues) {
        return this.start().with(memoryKeyValues);
    }

}
