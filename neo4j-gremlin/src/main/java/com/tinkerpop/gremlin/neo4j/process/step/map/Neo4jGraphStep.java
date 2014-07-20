package com.tinkerpop.gremlin.neo4j.process.step.map;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Pieter Martin
 */
public class Neo4jGraphStep<E extends Element> extends GraphStep<E> {

    private final Neo4jGraph graph;
    public final List<HasContainer> hasContainers = new ArrayList<>();

    public Neo4jGraphStep(final Traversal traversal, final Class<E> returnClass, final Neo4jGraph graph) {
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
        this.graph.tx().readWrite();
        final HasContainer indexedContainer = getEdgeIndexKey();
        final Stream<? extends Edge> edgeStream = (null == indexedContainer) ?
                getEdges() :
                getEdgesUsingIndex(indexedContainer);
        return edgeStream.filter(v -> HasContainer.testAll((Edge) v, this.hasContainers)).iterator();
    }

    private Iterator<? extends Vertex> vertices() {
        this.graph.tx().readWrite();
        Stream<? extends Vertex> vertexStream;
        if (this.hasContainers.size() > 1 && this.hasContainers.get(0).key.equals(Element.LABEL) && this.hasContainers.get(1).predicate.equals(Compare.EQUAL)) {
            //Scenario 1, using labeled index via 2 HasContainer
            HasContainer hasContainer1 = this.hasContainers.get(0);
            HasContainer hasContainer2 = this.hasContainers.get(1);
            //In this case neo4j will work out if there is an index or not
            this.hasContainers.remove(hasContainer1);
            this.hasContainers.remove(hasContainer2);
            vertexStream = getVerticesUsingLabeledIndex((String) hasContainer1.value, hasContainer2.key, hasContainer2.value);
        } else if (this.hasContainers.size() > 0 && this.hasContainers.get(0).key.equals(Element.LABEL)) {
            //Scenario 2, using label only for search
            HasContainer hasContainer1 = this.hasContainers.get(0);
            vertexStream = getVerticesUsingLabel((String) hasContainer1.value);
            this.hasContainers.remove(hasContainer1);
        } else  {
            HasContainer hasContainer1 = getVertexIndexKey();
            if (hasContainer1 != null) {
                vertexStream = getVerticesUsingLegacyIndex(hasContainer1.key, hasContainer1.value);
                this.hasContainers.remove(hasContainer1);
            } else {
                vertexStream = getVertices();
            }
        }
        return vertexStream.filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers)).iterator();
    }

    private Stream<Neo4jVertex> getVertices() {
        return StreamFactory.stream(GlobalGraphOperations.at(this.graph.getBaseGraph()).getAllNodes())
                .map(n -> new Neo4jVertex(n, this.graph));
    }

    private Stream<Neo4jEdge> getEdges() {
        return StreamFactory.stream(GlobalGraphOperations.at(this.graph.getBaseGraph()).getAllRelationships())
                .map(e -> new Neo4jEdge(e, this.graph));
    }

    private Stream<Vertex> getVerticesUsingLabeledIndex(final String label, String key, Object value) {
        ResourceIterator<Node> iterator = graph.getBaseGraph().findNodesByLabelAndProperty(DynamicLabel.label(label), key, value).iterator();
        return StreamFactory.stream(iterator).map(n -> new Neo4jVertex(n, this.graph));
    }

    private Stream<Vertex> getVerticesUsingLabel(final String label) {
        ResourceIterator<Node> iterator = GlobalGraphOperations.at(graph.getBaseGraph()).getAllNodesWithLabel(DynamicLabel.label(label)).iterator();
        return StreamFactory.stream(iterator).map(n -> new Neo4jVertex(n, this.graph));
    }

    private Stream<Vertex> getVerticesUsingLegacyIndex(String key, Object value) {
        final AutoIndexer indexer = this.graph.getBaseGraph().index().getNodeAutoIndexer();
        if (indexer.isEnabled() && indexer.getAutoIndexedProperties().contains(key))
            return StreamFactory.stream(this.graph.getBaseGraph().index().getNodeAutoIndexer().getAutoIndex().get(key, value).iterator())
                    .map(n -> new Neo4jVertex(n, this.graph));
        else
            throw new IllegalStateException("Index not here"); // todo: unecessary check/throw?
    }

    private Stream<Neo4jEdge> getEdgesUsingIndex(final HasContainer indexedContainer) {
        this.graph.tx().readWrite();
        final RelationshipAutoIndexer indexer = this.graph.getBaseGraph().index().getRelationshipAutoIndexer();
        return StreamFactory.stream(indexer.getAutoIndex().get(indexedContainer.key, indexedContainer.value).iterator())
                .map(e -> new Neo4jEdge(e, this.graph));
    }

    private HasContainer getVertexIndexKey() {
        final Set<String> indexedKeys = this.graph.getBaseGraph().index().getNodeAutoIndexer().getAutoIndexedProperties();
        HasContainer indexedHasContainer = this.hasContainers.stream()
                .filter(c -> (indexedKeys.contains(c.key) && c.predicate.equals(Compare.EQUAL)))
                .findFirst()
                .orElseGet(() -> null);
        return indexedHasContainer;
    }

    private HasContainer getEdgeIndexKey() {
        final Set<String> indexedKeys = this.graph.getBaseGraph().index().getRelationshipAutoIndexer().getAutoIndexedProperties();
        HasContainer indexedHasContainer = this.hasContainers.stream()
                .filter(c -> (indexedKeys.contains(c.key) && c.predicate.equals(Compare.EQUAL)))
                .findFirst()
                .orElseGet(() -> null);
        return indexedHasContainer;
    }


}
