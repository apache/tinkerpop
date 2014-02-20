package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.map.StartStep;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;

import java.util.function.Consumer;

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

    public Traversal<Vertex, Vertex> out(final int branchFactor, final String... labels);

    public Traversal<Vertex, Vertex> in(final int branchFactor, final String... labels);

    public Traversal<Vertex, Vertex> both(final int branchFactor, final String... labels);

    public Traversal<Vertex, Edge> outE(final int branchFactor, final String... labels);

    public Traversal<Vertex, Edge> inE(final int branchFactor, final String... labels);

    public Traversal<Vertex, Edge> bothE(final int branchFactor, final String... labels);

    ///////////////

    public default Traversal<Vertex, Vertex> out(final String... labels) {
        return this.out(Integer.MAX_VALUE, labels);
    }

    public default Traversal<Vertex, Vertex> in(final String... labels) {
        return this.in(Integer.MAX_VALUE, labels);
    }

    public default Traversal<Vertex, Vertex> both(final String... labels) {
        return this.both(Integer.MAX_VALUE, labels);
    }

    public default Traversal<Vertex, Edge> outE(final String... labels) {
        return this.outE(Integer.MAX_VALUE, labels);
    }

    public default Traversal<Vertex, Edge> inE(final String... labels) {
        return this.inE(Integer.MAX_VALUE, labels);
    }

    public default Traversal<Vertex, Edge> bothE(final String... labels) {
        return this.bothE(Integer.MAX_VALUE, labels);
    }

    ///////////////

    public default Traversal<Vertex, Vertex> start() {
        final Traversal<Vertex, Vertex> traversal = new DefaultTraversal<>();
        return traversal.addStep(new StartStep<>(traversal, this));
    }

    public default Traversal<Vertex, Vertex> as(final String as) {
        return this.start().as(as);
    }

    public default <E2> Traversal<Vertex, AnnotatedValue<E2>> annotatedValues(final String propertyKey) {
        return this.start().annotatedValues(propertyKey);
    }

    public default <E2> Traversal<Vertex, Property<E2>> property(final String propertyKey) {
        return this.start().property(propertyKey);
    }

    public default <E2> Traversal<Vertex, E2> value(final String propertyKey) {
        return this.start().value(propertyKey);
    }

    public default Traversal<Vertex, Vertex> with(final Object... variableValues) {
        return this.start().with(variableValues);
    }

    public default Traversal<Vertex, Vertex> sideEffect(final Consumer<Holder<Vertex>> consumer) {
        return this.start().sideEffect(consumer);
    }

    public static class Exceptions {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }
    }
}
