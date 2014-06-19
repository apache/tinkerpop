package com.tinkerpop.gremlin.structure.util.micro;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroVertex extends MicroElement implements Vertex {

    private MicroVertex() {

    }

    private MicroVertex(final Vertex vertex) {
        super(vertex);
    }

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new UnsupportedOperationException("Micro vertices do not store edges (inflate): " + this.toString());
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Vertex inflate(final Vertex hostVertex) {
        if (!hostVertex.id().toString().equals(this.id.toString())) // TODO: Why is this bad?
            throw new IllegalStateException("The host vertex must be the vertex trying to be inflated: " +
                    hostVertex.id() + "!=" + this.id() + " or " +
                    hostVertex.id().getClass() + "!=" + this.id().getClass());
        return hostVertex;
    }

    public Vertex inflate(final Graph graph) {
        return graph.v(this.id);
    }

    public static MicroVertex deflate(final Vertex vertex) {
        return new MicroVertex(vertex);
    }

    public GraphTraversal<Vertex, Vertex> as(final String as) {
        throw new IllegalStateException();
    }

    public GraphTraversal<Vertex, Edge> forE(final Direction direction, final int branchFactor, final String... labels) {
        throw new IllegalStateException();
    }

    public GraphTraversal<Vertex, Vertex> forV(final Direction direction, final int branchFactor, final String... labels) {
        throw new IllegalStateException();
    }

}
