package com.tinkerpop.gremlin.structure.util.referenced;

import com.tinkerpop.gremlin.structure.Edge;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferencedEdge extends ReferencedElement implements Edge {

    public ReferencedEdge() {

    }

    public ReferencedEdge(final Edge edge) {
        super(edge);
    }

    @Override
    public Edge.Iterators iterators() {
        throw new IllegalStateException("ReferencedEdges do not have iterators:" + this);
    }
}
