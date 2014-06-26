package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertex extends GiraphElement implements Vertex {

    public GiraphVertex(final Vertex vertex, final GiraphGraph graph) {
        super(vertex, graph);
    }

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw Vertex.Exceptions.edgeAdditionsNotSupported();
    }

    public GraphTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().to(direction, branchFactor, labels);
    }

    public GraphTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        return this.start().toE(direction, branchFactor, labels);
    }

    public GraphTraversal<Vertex, Vertex> start() {
        return this.graph.V().has(Element.ID, this.id());
    }
}
