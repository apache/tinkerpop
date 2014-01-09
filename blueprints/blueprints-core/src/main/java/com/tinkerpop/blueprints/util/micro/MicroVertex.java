package com.tinkerpop.blueprints.util.micro;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.Iterator;

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

    /*public Vertex macroSize(final Graph graph) {
        final Iterator<Vertex> vertices = graph.query().ids(this.id).vertices().iterator();
        if (vertices.hasNext())
            return vertices.next();
        else
            throw new IllegalStateException("The micro vertex doesn't have a corresponding vertex in the graph");
    }*/

}
