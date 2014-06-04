package com.tinkerpop.gremlin.structure.util.subgraph;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.function.Function;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SubgraphEdge implements Edge {

    private final Edge baseEdge;
    private final Function<Vertex, Boolean> vertexCriterion;
    private final Function<Edge, Boolean> edgeCriterion;

    public SubgraphEdge(final Edge baseEdge,
                        final Function<Vertex, Boolean> vertexCriterion,
                        final Function<Edge, Boolean> edgeCriterion) {
        this.baseEdge = baseEdge;
        this.vertexCriterion = vertexCriterion;
        this.edgeCriterion = edgeCriterion;
    }

    @Override
    public GraphTraversal<Edge, Vertex> inV() {
        return edgeCriterion.apply(baseEdge)
                ? new SubgraphTraversal<>(baseEdge.inV(), vertexCriterion, edgeCriterion, false, true)
                : new GraphTraversal.EmptyGraphTraversal<>();
    }

    @Override
    public GraphTraversal<Edge, Vertex> outV() {
        return edgeCriterion.apply(baseEdge)
                ? new SubgraphTraversal<>(baseEdge.outV(), vertexCriterion, edgeCriterion, false, true)
                : new GraphTraversal.EmptyGraphTraversal<>();
    }

    @Override
    public GraphTraversal<Edge, Vertex> bothV() {
        return edgeCriterion.apply(baseEdge)
                ? new SubgraphTraversal<>(baseEdge.bothV(), vertexCriterion, edgeCriterion, false, true)
                : new GraphTraversal.EmptyGraphTraversal<>();
    }

    @Override
    public Object id() {
        return baseEdge.id();
    }

    @Override
    public String label() {
        return baseEdge.label();
    }

    @Override
    public void remove() {
        baseEdge.remove();
    }

    @Override
    public Map<String, Property> properties() {
        return baseEdge.properties();
    }

    @Override
    public Map<String, Property> hiddens() {
        return baseEdge.hiddens();
    }

    @Override
    public <V> Property<V> property(final String key) {
        return baseEdge.property(key);
    }

    @Override
    public <V> Property<V> property(final String key,
                                    final V value) {
        return baseEdge.property(key, value);
    }
}
