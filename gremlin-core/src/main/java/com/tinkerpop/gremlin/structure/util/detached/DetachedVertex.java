package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedVertex extends DetachedElement implements Vertex {

    private DetachedVertex() {

    }

    protected DetachedVertex(final Object id, final String label) {
        super(id, label);
    }

    public DetachedVertex(final Object id, final String label, final Map<String, Object> properties, final Map<String, Object> hiddenProperties) {
        super(id, label, properties, hiddenProperties);
    }

    private DetachedVertex(final Vertex vertex) {
        super(vertex);
    }

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new UnsupportedOperationException("Detached vertices do not store edges: " + this);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public GraphTraversal<Vertex, Vertex> as(final String as) {
        throw new IllegalStateException();
    }

    public GraphTraversal<Vertex, Edge> edges(final Direction direction, final int branchFactor, final String... labels) {
        throw new IllegalStateException();
    }

    public GraphTraversal<Vertex, Vertex> vertices(final Direction direction, final int branchFactor, final String... labels) {
        throw new IllegalStateException();
    }

    public Vertex attach(final Vertex hostVertex) {
        if (!hostVertex.id().toString().equals(this.id.toString())) // TODO: Why is this bad?
            throw new IllegalStateException("The host vertex must be the vertex trying to be attached: " +
                    hostVertex.id() + "!=" + this.id() + " or " +
                    hostVertex.id().getClass() + "!=" + this.id().getClass());
        return hostVertex;
    }

    public Vertex attach(final Graph graph) {
        return graph.v(this.id);
    }

    public static DetachedVertex detach(final Vertex vertex) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        return new DetachedVertex(vertex);
    }
}
