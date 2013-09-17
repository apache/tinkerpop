package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;

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

    public Iterable<Edge> edges() {
        return graph.edges.values().parallelStream().filter(v -> HasContainer.testAll(v, this.hasContainers)).limit(this.limit).collect(Collectors.<Edge>toList());
    }

    public Iterable<Vertex> vertices() {
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
