package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;

import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphQuery extends DefaultGraphQuery {

    private final TinkerGraph graph;

    public TinkerGraphQuery(final TinkerGraph graph) {
        this.graph = graph;
    }

    public Iterable<Edge> edges() {
        return graph.edges.values().stream().filter(v -> HasContainer.testAll(v, this.hasContainers)).limit(this.limit).collect(Collectors.<Edge>toList());
    }

    public Iterable<Vertex> vertices() {
        return graph.vertices.values().stream().filter(v -> HasContainer.testAll(v, this.hasContainers)).limit(this.limit).collect(Collectors.<Vertex>toList());
    }
}
