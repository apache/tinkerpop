package com.tinkerpop.gremlin.structure.util.micro;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroEdge extends MicroElement implements Edge {

    MicroVertex outVertex;
    MicroVertex inVertex;

    private MicroEdge() {

    }

    private MicroEdge(final Edge edge) {
        super(edge);
        this.outVertex = MicroVertex.deflate(edge.outV().next());
        this.inVertex = MicroVertex.deflate(edge.inV().next());
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    public Edge inflate(final Vertex hostVertex) {
        return StreamFactory.stream((Iterator<Edge>) hostVertex.outE(this.label))
                .filter(e -> e.id().equals(this.id))
                .findFirst().orElseThrow(() -> new IllegalStateException("The micro edge could not be be found at the provided vertex"));
    }

    public Edge inflate(final Graph graph) {
        return graph.e(this.id);
    }

    public static MicroEdge deflate(final Edge edge) {
        return new MicroEdge(edge);
    }

    public GraphTraversal<Edge, Vertex> inV() {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new MicroEdgeVertexStep(traversal, Direction.IN));
        return traversal;
    }

    public GraphTraversal<Edge, Vertex> outV() {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new MicroEdgeVertexStep(traversal, Direction.OUT));
        return traversal;
    }

    public GraphTraversal<Edge, Vertex> bothV() {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new MicroEdgeVertexStep(traversal, Direction.BOTH));
        return traversal;
    }

    class MicroEdgeVertexStep extends EdgeVertexStep {
        public MicroEdgeVertexStep(final Traversal traversal, final Direction direction) {
            super(traversal, direction);
            this.setFunction(traverser -> {
                final List<Vertex> vertices = new ArrayList<>();
                if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
                    vertices.add(outVertex);
                if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
                    vertices.add(inVertex);

                return vertices.iterator();
            });
        }
    }
}
