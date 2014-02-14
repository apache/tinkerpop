package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Traversal;

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

    public <A extends Traversal<Vertex, Vertex>> A as(final String as);

    public default <A extends Traversal<Vertex, Vertex>> A out(final String... labels) {
        return this.out(Integer.MAX_VALUE, labels);
    }

    public default <A extends Traversal<Vertex, Vertex>> A in(final String... labels) {
        return this.in(Integer.MAX_VALUE, labels);
    }

    public default <A extends Traversal<Vertex, Vertex>> A both(final String... labels) {
        return this.both(Integer.MAX_VALUE, labels);
    }

    public <A extends Traversal<Vertex, Vertex>> A out(final int branchFactor, final String... labels);

    public <A extends Traversal<Vertex, Vertex>> A in(final int branchFactor, final String... labels);

    public <A extends Traversal<Vertex, Vertex>> A both(final int branchFactor, final String... labels);

    public default <A extends Traversal<Vertex, Edge>> A outE(final String... labels) {
        return this.outE(Integer.MAX_VALUE, labels);
    }

    public default <A extends Traversal<Vertex, Edge>> A inE(final String... labels) {
        return this.inE(Integer.MAX_VALUE, labels);
    }

    public default <A extends Traversal<Vertex, Edge>> A bothE(final String... labels) {
        return this.bothE(Integer.MAX_VALUE, labels);
    }

    public <A extends Traversal<Vertex, Edge>> A outE(final int branchFactor, final String... labels);

    public <A extends Traversal<Vertex, Edge>> A inE(final int branchFactor, final String... labels);

    public <A extends Traversal<Vertex, Edge>> A bothE(final int branchFactor, final String... labels);

    public static class Exceptions {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }
    }
}
