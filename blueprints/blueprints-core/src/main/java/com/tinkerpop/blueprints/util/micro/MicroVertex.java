package com.tinkerpop.blueprints.util.micro;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroVertex extends MicroElement implements Vertex {

    private MicroVertex(final Vertex vertex) {
        super(vertex);
    }

    public VertexQuery query() {
        throw new UnsupportedOperationException("Micro vertices can not be queried (inflate): " + this.toString());
    }

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new UnsupportedOperationException("Micro vertices do not store edges (inflate): " + this.toString());
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Vertex inflate(final Vertex hostVertex) {
        if (!hostVertex.getId().equals(this.id))
            throw new IllegalStateException("The host vertex must be the vertex trying to be inflated: " + hostVertex.getId() + "!=" + this.getId());
        return hostVertex;
    }

    public Vertex inflate(final Graph graph) {
        return graph.query().ids(this.id).vertices().iterator().next();
    }

    public static MicroVertex deflate(final Vertex vertex) {
        return new MicroVertex(vertex);
    }

}
