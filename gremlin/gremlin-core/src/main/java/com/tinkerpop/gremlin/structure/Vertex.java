package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.query.VertexQuery;

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

    public VertexQuery query();

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues);

    public <A extends Traversal<Vertex, Vertex>> A out(final String... labels);

    public <A extends Traversal<Vertex, Vertex>> A in(final String... labels);

    public <A extends Traversal<Vertex, Vertex>> A both(final String... labels);

    public static class Exceptions {
        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }
    }
}
