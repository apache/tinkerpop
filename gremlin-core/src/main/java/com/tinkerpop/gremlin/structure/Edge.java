package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.util.function.SBiPredicate;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.Iterator;
import java.util.Map;

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

    // element steps ///////////////////////////////////////////////////////////

    public default GraphTraversal<Edge, Vertex> toV(final Direction direction) {
        return this.start().toV(direction);
    }

    public default GraphTraversal<Edge, Vertex> inV() {
        return this.toV(Direction.IN);
    }

    public default GraphTraversal<Edge, Vertex> outV() {
        return this.toV(Direction.OUT);
    }

    public default GraphTraversal<Edge, Vertex> bothV() {
        return this.toV(Direction.BOTH);
    }

    public default GraphTraversal<Edge, Edge> aggregate(final String variable, final SFunction<Edge, ?> preAggregateFunction) {
        return this.start().aggregate(variable, preAggregateFunction);
    }

    public default GraphTraversal<Edge, Edge> aggregate(final SFunction<Edge, ?> preAggregateFunction) {
        return this.start().aggregate(preAggregateFunction);
    }

    public default GraphTraversal<Edge, Edge> aggregate(final String variable) {
        return this.start().aggregate(variable);
    }

    public default GraphTraversal<Edge, Edge> aggregate() {
        return this.start().aggregate();
    }

    public default GraphTraversal<Edge, Edge> store(final String variable, final SFunction<Edge, ?> preStoreFunction) {
        return this.start().store(variable, preStoreFunction);
    }

    public default GraphTraversal<Edge, Edge> store(final String variable) {
        return this.start().store(variable);
    }

    public default GraphTraversal<Edge, Edge> store(final SFunction<Edge, ?> preStoreFunction) {
        return this.start().store(preStoreFunction);
    }

    public default GraphTraversal<Edge, Edge> store() {
        return this.start().store();
    }

    public default GraphTraversal<Edge, Edge> as(final String as) {
        return this.start().as(as);
    }

    public default GraphTraversal<Edge, Edge> filter(final SPredicate<Traverser<Edge>> predicate) {
        return this.start().filter(predicate);
    }

    public default <E2> GraphTraversal<Edge, E2> flatMap(final SFunction<Traverser<Edge>, Iterator<E2>> function) {
        return this.start().flatMap(function);
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

    public default GraphTraversal<Edge, Edge> identity() {
        return this.start().identity();
    }

    public default GraphTraversal<Edge, Edge> jump(final String as) {
        return this.start().jump(as);
    }

    public default GraphTraversal<Edge, Edge> jump(final String as, final SPredicate<Traverser<Edge>> ifPredicate) {
        return this.start().jump(as, ifPredicate);
    }

    public default GraphTraversal<Edge, Edge> jump(final String as, final SPredicate<Traverser<Edge>> ifPredicate, final SPredicate<Traverser<Edge>> emitPredicate) {
        return this.start().jump(as, ifPredicate, emitPredicate);
    }

    public default <E2> GraphTraversal<Edge, E2> map(final SFunction<Traverser<Edge>, E2> function) {
        return this.start().map(function);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> match(final String inAs, final Traversal... traversals) {
        return (GraphTraversal) this.start().match(inAs, traversals);
    }

    public default GraphTraversal<Edge, Edge> sideEffect(final SConsumer<Traverser<Edge>> consumer) {
        return this.start().sideEffect(consumer);
    }

    public default <E2> GraphTraversal<Edge, E2> choose(final SPredicate<Traverser<Edge>> choosePredicate, final Traversal trueChoice, final Traversal falseChoice) {
        return this.start().choose(choosePredicate, trueChoice, falseChoice);
    }

    public default <E2, M> GraphTraversal<Edge, E2> choose(final SFunction<Traverser<Edge>, M> mapFunction, final Map<M, Traversal<Edge, E2>> choices) {
        return this.start().choose(mapFunction, choices);
    }

    public default GraphTraversal<Edge, Edge> with(final Object... variableValues) {
        return this.start().with(variableValues);
    }

    public default GraphTraversal<Edge, Edge> start() {
        final GraphTraversal<Edge, Edge> traversal = GraphTraversal.of();
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    ///////////////

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
}
