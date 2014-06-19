package com.tinkerpop.gremlin.structure.util.subgraph;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.function.Function;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SubGraphVertex implements Vertex {

    private final Vertex baseVertex;
    private final Function<Vertex, Boolean> vertexCriterion;
    private final Function<Edge, Boolean> edgeCriterion;

    public SubGraphVertex(final Vertex baseVertex,
                          final Function<Vertex, Boolean> vertexCriterion,
                          final Function<Edge, Boolean> edgeCriterion) {
        this.baseVertex = baseVertex;
        this.vertexCriterion = vertexCriterion;
        this.edgeCriterion = edgeCriterion;
    }

    @Override
    public Edge addEdge(final String label,
                        final Vertex inVertex,
                        final Object... keyValues) {
        // note: created edge may not pass the edgeCriterion
        return new SubGraphEdge(baseVertex.addEdge(label, ((SubGraphVertex) inVertex).baseVertex, keyValues), vertexCriterion, edgeCriterion);
    }

    @Override
    public GraphTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        return vertexCriterion.apply(baseVertex)
                ? new SubGraphTraversal<>(
                new SubGraphEdgeToVertexTraversal(baseVertex.toE(direction, branchFactor, labels), baseVertex, edgeCriterion, direction.opposite()),
                vertexCriterion, edgeCriterion, true)
                : new GraphTraversal.EmptyGraphTraversal<>();
    }

    @Override
    public GraphTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        return vertexCriterion.apply(baseVertex)
                ? new SubGraphTraversal<>(baseVertex.toE(direction, branchFactor, labels), vertexCriterion, edgeCriterion, false)
                : new GraphTraversal.EmptyGraphTraversal<>();
    }

    @Override
    public Object id() {
        return baseVertex.id();
    }

    @Override
    public String label() {
        return baseVertex.label();
    }

    @Override
    public void remove() {
        baseVertex.remove();
    }

    @Override
    public Map<String, Property> properties() {
        return baseVertex.properties();
    }

    @Override
    public Map<String, Property> hiddens() {
        return baseVertex.properties();
    }

    @Override
    public <V> Property<V> property(final String key) {
        return baseVertex.property(key);
    }

    @Override
    public <V> Property<V> property(final String key,
                                    final V value) {
        return baseVertex.property(key, value);
    }
}
