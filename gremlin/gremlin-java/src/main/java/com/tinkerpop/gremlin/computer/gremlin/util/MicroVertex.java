package com.tinkerpop.gremlin.computer.gremlin.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroVertex extends MicroElement implements Vertex {

    public MicroVertex(final Vertex vertex) {
        super(vertex);
    }

    public VertexQuery query() {
        throw new UnsupportedOperationException();
    }

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return StringFactory.vertexString(this) + ".";
    }

}
