package com.tinkerpop.gremlin.structure.util.cached;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CachedEdge extends CachedElement implements Edge {
    private CachedVertex outVertex;
    private CachedVertex inVertex;

    public CachedEdge(final Object id, final String label, final Map<String, Object> properties,
					  final Map<String, Object> hiddenProperties,
                      final Pair<Object, String> outV, final Pair<Object, String> inV) {
        super(id, label, properties, hiddenProperties);
        this.outVertex = new CachedVertex(outV.getValue0(), outV.getValue1());
        this.inVertex = new CachedVertex(inV.getValue0(), inV.getValue1());
    }

    public CachedEdge(final Edge edge) {
        super(edge);
        final Vertex ov = edge.outV().next();
        final Vertex iv = edge.inV().next();
        this.outVertex = new CachedVertex(ov.id(), ov.label());
        this.inVertex = new CachedVertex(iv.id(), iv.label());
    }

    @Override
    public GraphTraversal<Edge, Vertex> inV() {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new CachedEdgeVertexStep(traversal, Direction.IN));
        return traversal;
    }

    @Override
    public GraphTraversal<Edge, Vertex> outV() {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new CachedEdgeVertexStep(traversal, Direction.OUT));
        return traversal;
    }

    @Override
    public GraphTraversal<Edge, Vertex> bothV() {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new CachedEdgeVertexStep(traversal, Direction.BOTH));
        return traversal;
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    class CachedEdgeVertexStep extends EdgeVertexStep {
        public CachedEdgeVertexStep(final Traversal traversal, final Direction direction) {
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
