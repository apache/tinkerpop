package com.tinkerpop.gremlin.tinkergraph.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
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
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphStep<E extends Element> extends GraphStep<E> {

    private TinkerGraph graph;
    public final List<HasContainer> hasContainers = new ArrayList<>();

    public TinkerGraphStep(final Traversal traversal, final Class<E> returnClass, final TinkerGraph graph) {
        super(traversal, returnClass);
        this.graph = graph;
    }

    public void generateTraverserIterator(final boolean trackPaths) {
        this.starts.clear();
        if (trackPaths)
            this.starts.add(new TraverserIterator(this, Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
        else
            this.starts.add(new TraverserIterator(Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));

    }

    public void clear() {
        this.starts.clear();
    }

    private Iterator<? extends Edge> edges() {
        final HasContainer indexedContainer = getIndexKey(Edge.class);
        final Stream<? extends Edge> edgeStream = (null == indexedContainer) ?
                TinkerHelper.getEdges(this.graph).stream() :
                TinkerHelper.queryEdgeIndex(this.graph, indexedContainer.key, indexedContainer.value).stream();
        return (Iterator) edgeStream.filter(e -> HasContainer.testAll((Edge) e, this.hasContainers)).collect(java.util.stream.Collectors.toList()).iterator();

        // todo: concurrency errors when we maintain laziness
        //return edgeStream.filter(e -> HasContainer.testAll((Edge) e, this.hasContainers)).iterator();
    }

    private Iterator<? extends Vertex> vertices() {
        final HasContainer indexedContainer = getIndexKey(Vertex.class);
        final Stream<? extends Vertex> vertexStream = (null == indexedContainer) ?
                TinkerHelper.getVertices(this.graph).stream() :
                TinkerHelper.queryVertexIndex(this.graph, indexedContainer.key, indexedContainer.value).stream();
        return (Iterator) vertexStream.filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers)).collect(java.util.stream.Collectors.toList()).iterator();

        // todo: concurrency errors when we maintain laziness
        // return vertexStream.filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers)).iterator();
    }

    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
        final Set<String> indexedKeys = this.graph.getIndexedKeys(indexedClass);
        return this.hasContainers.stream()
                .filter(c -> indexedKeys.contains(c.key) && c.predicate.equals(Compare.EQUAL))
                .findFirst()
                .orElseGet(() -> null);
    }

}
