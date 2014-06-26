package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphEdge extends GiraphElement implements Edge {

    public GiraphEdge(final Edge edge, final GiraphGraph graph) {
        super(edge, graph);
    }

    public GraphTraversal<Edge, Vertex> toV(final Direction direction) {
        return this.start().toV(direction);
    }

    public GraphTraversal<Edge, Edge> start() {
        return this.graph.E().has(Element.ID, this.id());
    }
}