package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.VertexQuery;

/**
 * A vertex maintains pointers to both a set of incoming and outgoing edges. The outgoing edges are those edges for
 * which the vertex is the tail. The incoming edges are those edges for which the vertex is the head.
 * <p/>
 * Diagrammatically, ---inEdges---> vertex ---outEdges--->.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Vertex extends Element {

    public VertexQuery query();

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues);

}
