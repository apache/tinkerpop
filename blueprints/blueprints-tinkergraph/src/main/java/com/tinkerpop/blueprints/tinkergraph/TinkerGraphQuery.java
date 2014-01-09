package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Strategy;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.GraphQuery;
import com.tinkerpop.blueprints.query.util.DefaultGraphQuery;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphQuery extends DefaultGraphQuery {

    private final TinkerGraph graph;

    public TinkerGraphQuery(final TinkerGraph graph) {
        this.graph = graph;
    }

    @Override
    public GraphQuery ids(final Object... ids) {
        return this.graph.strategy().compose(s -> s.getGraphQueryIdsStrategy(new Strategy.Context<GraphQuery>(this.graph, this)), super::ids).apply(ids);
    }

    public Iterable<Edge> edges() {
        final HasContainer indexedContainer = getIndexKey(Edge.class);
        return ((null == indexedContainer) ?
                this.graph.edges.values().parallelStream() :
                this.graph.edgeIndex.get(indexedContainer.key, indexedContainer.value).parallelStream())
                .filter(e -> HasContainer.testAll((Edge) e, this.hasContainers))
                .limit(this.limit)
                .collect(Collectors.<Edge>toList());
    }

    public Iterable<Vertex> vertices() {
        return this.graph.strategy().compose(s -> s.getGraphQueryVerticesStrategy(new Strategy.Context<GraphQuery>(this.graph, this)), this::internalVertices).get();
    }

    private Iterable<Vertex> internalVertices() {
        final HasContainer indexedContainer = getIndexKey(Vertex.class);
        return ((null == indexedContainer) ?
                this.graph.vertices.values().parallelStream() :
                this.graph.vertexIndex.get(indexedContainer.key, indexedContainer.value).parallelStream())
                .filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers))
                .limit(this.limit)
                .collect(Collectors.<Vertex>toList());
    }

    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
        final Set<String> indexedKeys = this.graph.getIndexedKeys(indexedClass);
        return this.hasContainers.stream()
                .filter(c -> indexedKeys.contains(c.key) && c.predicate.equals(Compare.EQUAL))
                .findFirst()
                .orElseGet(() -> null);
    }
}
