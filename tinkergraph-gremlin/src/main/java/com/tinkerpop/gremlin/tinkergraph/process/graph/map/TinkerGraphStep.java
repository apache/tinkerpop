package com.tinkerpop.gremlin.tinkergraph.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.GraphStep;
import com.tinkerpop.gremlin.process.util.HolderIterator;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphStep<E extends Element> extends GraphStep<E> {

    private TinkerGraph graph;
    public final List<HasContainer> hasContainers = new ArrayList<>();

    public TinkerGraphStep(final Traversal traversal, final Class<E> returnClass, final TinkerGraph graph) {
        super(traversal, returnClass);
        this.graph = graph;
        this.generateHolderIterator(false);
    }

    public void generateHolderIterator(final boolean trackPaths) {
        this.starts.clear();
        if (trackPaths)
            this.starts.add(new HolderIterator(this, Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
        else
            this.starts.add(new HolderIterator(Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));

    }

    //TODO: Remove if not needed
    public void clearGraph() {
        this.graph = null;
        this.starts = null;
    }

    public void clear() {
        this.starts.clear();
    }

    private Iterator<Edge> edges() {
        final HasContainer indexedContainer = getIndexKey(Edge.class);
        return (Iterator) ((null == indexedContainer) ?
                TinkerHelper.getEdges(this.graph).parallelStream() :
                TinkerHelper.queryEdgeIndex(this.graph, indexedContainer.key, indexedContainer.value).parallelStream())
                .filter(e -> HasContainer.testAll((Edge) e, this.hasContainers)).collect(Collectors.toList()).iterator();
    }

    private Iterator<Vertex> vertices() {
        final HasContainer indexedContainer = getIndexKey(Vertex.class);
        return (Iterator) ((null == indexedContainer) ?
                TinkerHelper.getVertices(this.graph).parallelStream() :
                TinkerHelper.queryVertexIndex(this.graph, indexedContainer.key, indexedContainer.value).parallelStream())
                .filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers)).collect(Collectors.toList()).iterator();
    }

    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
        final Set<String> indexedKeys = this.graph.getIndexedKeys(indexedClass);
        return this.hasContainers.stream()
                .filter(c -> indexedKeys.contains(c.key) && c.predicate.equals(Compare.EQUAL))
                .findFirst()
                .orElseGet(() -> null);
    }

}
