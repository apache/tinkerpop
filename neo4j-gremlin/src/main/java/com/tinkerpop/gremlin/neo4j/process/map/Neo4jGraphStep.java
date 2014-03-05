package com.tinkerpop.gremlin.neo4j.process.map;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.GraphStep;
import com.tinkerpop.gremlin.process.util.HolderIterator;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jGraphStep<E extends Element> extends GraphStep<E> {

    private final Neo4jGraph graph;
    public final List<HasContainer> hasContainers = new ArrayList<>();

    public Neo4jGraphStep(final Traversal traversal, final Class<E> returnClass, final Neo4jGraph graph) {
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

    private Iterator<Edge> edges() {
        this.graph.autoStartTransaction(false);
        final HasContainer indexedContainer = getIndexKey(Edge.class);
        return (Iterator) ((null == indexedContainer) ?
                StreamFactory.parallelStream(GlobalGraphOperations.at(this.graph.getRawGraph()).getAllRelationships()) :
                getEdgesUsingIndex(indexedContainer).parallel())
                .filter(e -> HasContainer.testAll((Edge) e, this.hasContainers)).collect(Collectors.toList()).iterator();
    }

    private Iterator<Vertex> vertices() {
        this.graph.autoStartTransaction(false);

        final HasContainer indexedContainer = getIndexKey(Vertex.class);
        return (Iterator) ((null == indexedContainer) ?
                StreamFactory.parallelStream(GlobalGraphOperations.at(this.graph.getRawGraph()).getAllNodes()) :
                getVerticesUsingIndex(indexedContainer).parallel())
                .filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers)).collect(Collectors.toList()).iterator();
    }

    private Stream<Neo4jVertex> getVerticesUsingIndex(final HasContainer indexedContainer) {
        this.graph.autoStartTransaction(false);
        final AutoIndexer indexer = this.graph.getRawGraph().index().getNodeAutoIndexer();
        if (indexer.isEnabled() && indexer.getAutoIndexedProperties().contains(indexedContainer.key))
            return StreamFactory.stream(this.graph.getRawGraph().index().getNodeAutoIndexer().getAutoIndex().get(indexedContainer.key, indexedContainer.value).iterator())
                            .map(n -> new Neo4jVertex(n, this.graph));
        else
            throw new IllegalStateException("Index not here"); // todo: unecessary check/throw?
    }

    private Stream<Neo4jEdge> getEdgesUsingIndex(final HasContainer indexedContainer) {
        this.graph.autoStartTransaction(false);
        final AutoIndexer indexer = this.graph.getRawGraph().index().getNodeAutoIndexer();
        if (indexer.isEnabled() && indexer.getAutoIndexedProperties().contains(indexedContainer.key))
            return StreamFactory.stream(this.graph.getRawGraph().index().getRelationshipAutoIndexer().getAutoIndex().get(indexedContainer.key, indexedContainer.value).iterator())
                    .map(e -> new Neo4jEdge(e, this.graph));
        else
            throw new IllegalStateException("Index not here"); // todo: unecessary check/throw?
    }

    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
        this.graph.autoStartTransaction(false);

        // todo: review this stuff in comparison to tinkergraph
        final Set<String> indexedKeys;
        if (indexedClass.isAssignableFrom(Vertex.class))
            indexedKeys = new HashSet<>(Arrays.asList(this.graph.getRawGraph().index().nodeIndexNames()));
        else if (indexedClass.isAssignableFrom(Edge.class))
            indexedKeys = new HashSet<>(Arrays.asList(this.graph.getRawGraph().index().relationshipIndexNames()));
        else
            throw new RuntimeException("Indexes must be related to a Vertex or an Edge");

        return this.hasContainers.stream()
                .filter(c -> indexedKeys.contains(c.key) && c.predicate.equals(Compare.EQUAL))
                .findFirst()
                .orElseGet(() -> null);
    }

}
