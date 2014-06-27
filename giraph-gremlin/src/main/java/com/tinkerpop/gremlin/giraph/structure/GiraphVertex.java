package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.graph.step.map.GiraphVertexStep;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
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
        final GraphTraversal traversal = this.start();
        return (GraphTraversal) traversal.addStep(new GiraphVertexStep(traversal, this.graph, Vertex.class, direction, branchFactor, labels));

    }

    public GraphTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        final GraphTraversal traversal = this.start();
        return (GraphTraversal) traversal.addStep(new GiraphVertexStep(traversal, this.graph, Edge.class, direction, branchFactor, labels));
    }

    public GraphTraversal<Vertex, Vertex> start() {
        final GraphTraversal traversal = new DefaultGraphTraversal<>();
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public Vertex getRawVertex() {
        return (Vertex) this.element;
    }
}
