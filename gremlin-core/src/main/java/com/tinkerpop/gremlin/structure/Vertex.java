package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.map.StartStep;
import com.tinkerpop.gremlin.util.function.SConsumer;
import com.tinkerpop.gremlin.util.function.SFunction;

/**
 * A {@link Vertex} maintains pointers to both a set of incoming and outgoing {@link Edge} objects. The outgoing edges
 * are those edges for  which the {@link Vertex} is the tail. The incoming edges are those edges for which the
 * {@link Vertex} is the head.
 * <p/>
 * Diagrammatically:
 * <pre>
 * ---inEdges---> vertex ---outEdges--->.
 * </pre>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Vertex extends Element {

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues);

    public GraphTraversal<Vertex, Vertex> out(final int branchFactor, final String... labels);

    public GraphTraversal<Vertex, Vertex> in(final int branchFactor, final String... labels);

    public GraphTraversal<Vertex, Vertex> both(final int branchFactor, final String... labels);

    public GraphTraversal<Vertex, Edge> outE(final int branchFactor, final String... labels);

    public GraphTraversal<Vertex, Edge> inE(final int branchFactor, final String... labels);

    public GraphTraversal<Vertex, Edge> bothE(final int branchFactor, final String... labels);

    ///////////////

    public default GraphTraversal<Vertex, Vertex> out(final String... labels) {
        return this.out(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Vertex> in(final String... labels) {
        return this.in(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Vertex> both(final String... labels) {
        return this.both(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Edge> outE(final String... labels) {
        return this.outE(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Edge> inE(final String... labels) {
        return this.inE(Integer.MAX_VALUE, labels);
    }

    public default GraphTraversal<Vertex, Edge> bothE(final String... labels) {
        return this.bothE(Integer.MAX_VALUE, labels);
    }

    ///////////////

    public default GraphTraversal<Vertex, Vertex> start() {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>();
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public default GraphTraversal<Vertex, Vertex> aggregate(final String variable, final SFunction... preAggregateFunctions) {
        return (GraphTraversal) this.start().aggregate(variable, preAggregateFunctions);
    }

    public default GraphTraversal<Vertex, Vertex> as(final String as) {
        return (GraphTraversal) this.start().as(as);
    }

    public default GraphTraversal<Vertex, Vertex> identity() {
        return (GraphTraversal) this.start().identity();
    }

    public default <E2> GraphTraversal<Vertex, AnnotatedValue<E2>> annotatedValues(final String propertyKey) {
        return (GraphTraversal) this.start().annotatedValues(propertyKey);
    }

    public default <E2> GraphTraversal<Vertex, Property<E2>> property(final String propertyKey) {
        return (GraphTraversal) this.start().property(propertyKey);
    }

    /**
     * public default <E2> GraphTraversal<Vertex, Property<E2>> properties() {
     * return (GraphTraversal) this.start().values;
     * }*
     */

    public default <E2> GraphTraversal<Vertex, E2> value(final String propertyKey) {
        return (GraphTraversal) this.start().value(propertyKey);
    }

    public default GraphTraversal<Vertex, Vertex> with(final Object... variableValues) {
        return (GraphTraversal) this.start().with(variableValues);
    }

    public default GraphTraversal<Vertex, Vertex> sideEffect(final SConsumer<Holder<Vertex>> consumer) {
        return (GraphTraversal) this.start().sideEffect(consumer);
    }

    public static class Exceptions {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }
    }
}
