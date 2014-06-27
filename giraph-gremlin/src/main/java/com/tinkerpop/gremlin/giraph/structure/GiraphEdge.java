package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.graph.step.map.GiraphEdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphEdge extends GiraphElement implements Edge {

    public GiraphEdge(final Edge edge, final GiraphGraph graph) {
        super(edge, graph);
    }

    public GraphTraversal<Edge, Vertex> toV(final Direction direction) {
        final GraphTraversal traversal = this.start();
        return (GraphTraversal) traversal.addStep(new GiraphEdgeVertexStep(traversal, this.graph, direction));

    }

    public GraphTraversal<Edge, Edge> start() {
        final GraphTraversal traversal = new DefaultGraphTraversal<>();
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public Edge getRawEdge() {
        return (Edge) this.element;
    }
}