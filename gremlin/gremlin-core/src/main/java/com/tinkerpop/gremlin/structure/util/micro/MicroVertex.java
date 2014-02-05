package com.tinkerpop.gremlin.structure.util.micro;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.VertexQuery;
import com.tinkerpop.gremlin.structure.util.StringFactory;

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
        return graph.v(this.id).orElseThrow(() -> new IllegalStateException("The micro vertex could not be found at the provided graph"));
    }

    public static MicroVertex deflate(final Vertex vertex) {
        return new MicroVertex(vertex);
    }

}
