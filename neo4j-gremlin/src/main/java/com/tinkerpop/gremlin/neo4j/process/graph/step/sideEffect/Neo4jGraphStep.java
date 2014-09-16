package com.tinkerpop.gremlin.neo4j.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jMetaProperty;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    @Override
    public void generateTraverserIterator(final boolean trackPaths) {
        this.start = Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges();
        //makeCypherQuery();
        super.generateTraverserIterator(trackPaths);
    }

    private Iterator<? extends Edge> edges() {
        this.graph.tx().readWrite();
        final HasContainer indexedContainer = getEdgeIndexKey();
        final Stream<? extends Edge> edgeStream = (null == indexedContainer) ?
                getAllEdges() :
                getEdgesUsingIndex(indexedContainer);
        return edgeStream.filter(edge -> HasContainer.testAll((Edge) edge, this.hasContainers)).iterator();
    }

    private Iterator<? extends Vertex> vertices() {
        this.graph.tx().readWrite();
        final Stream<? extends Vertex> vertexStream;
        if (this.hasContainers.size() > 1 && this.hasContainers.get(0).key.equals(Element.LABEL) && this.hasContainers.get(1).predicate.equals(Compare.EQUAL)) {
            //Scenario 1, using labeled index via 2 HasContainer
            final HasContainer hasContainer1 = this.hasContainers.get(0);
            final HasContainer hasContainer2 = this.hasContainers.get(1);
            //In this case neo4j will work out if there is an index or not
            this.hasContainers.remove(hasContainer1);
            this.hasContainers.remove(hasContainer2);
            vertexStream = getVerticesUsingLabeledIndex((String) hasContainer1.value, hasContainer2.key, hasContainer2.value);
        } else if (this.hasContainers.size() > 0 && this.hasContainers.get(0).key.equals(Element.LABEL)) {
            //Scenario 2, using label only for search
            final HasContainer hasContainer1 = this.hasContainers.get(0);
            if (hasContainer1.predicate == Contains.IN || hasContainer1.predicate == Contains.NOT_IN) {
                final List<String> labels = (List<String>) hasContainer1.value;
                vertexStream = getVerticesUsingLabel(labels.toArray(new String[labels.size()]));
            } else
                vertexStream = getVerticesUsingLabel(hasContainer1.value.toString());

            this.hasContainers.remove(hasContainer1);
        } else {
            final HasContainer hasContainer1 = getVertexIndexKey();
            if (hasContainer1 != null) {
                vertexStream = getVerticesUsingLegacyIndex(hasContainer1.key, hasContainer1.value);
                this.hasContainers.remove(hasContainer1);
            } else {
                vertexStream = getAllVertices();
            }
        }
        return vertexStream.filter(vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers)).iterator();
    }

    private Stream<Neo4jVertex> getAllVertices() {
        return StreamFactory.stream(GlobalGraphOperations.at(this.graph.getBaseGraph()).getAllNodes())
                .filter(node -> !node.getLabels().iterator().next().equals(Neo4jMetaProperty.META_PROPERTY_LABEL))
                .map(node -> new Neo4jVertex(node, this.graph));
    }

    private Stream<Neo4jEdge> getAllEdges() {
        return StreamFactory.stream(GlobalGraphOperations.at(this.graph.getBaseGraph()).getAllRelationships())
                .filter(relationship -> !relationship.getType().name().startsWith(Neo4jMetaProperty.META_PROPERTY_PREFIX))
                .map(relationship -> new Neo4jEdge(relationship, this.graph));
    }

    private Stream<Vertex> getVerticesUsingLabeledIndex(final String label, String key, Object value) {
        final ResourceIterator<Node> iterator1 = graph.getBaseGraph().findNodesByLabelAndProperty(DynamicLabel.label(label), key, value).iterator();
        final ResourceIterator<Node> iterator2 = graph.getBaseGraph().findNodesByLabelAndProperty(Neo4jMetaProperty.META_PROPERTY_LABEL, key, value).iterator();
        final Stream<Vertex> stream1 = StreamFactory.stream(iterator1)
                .map(node -> new Neo4jVertex(node, this.graph));
        final Stream<Vertex> stream2 = StreamFactory.stream(iterator2)
                .map(node -> node.getRelationships(Direction.INCOMING).iterator().next().getStartNode())
                .map(node -> new Neo4jVertex(node, this.graph));
        return Stream.concat(stream1, stream2);
    }

    private Stream<Vertex> getVerticesUsingLabel(final String... labels) {
        return Arrays.stream(labels)
                .filter(label -> !label.equals(Neo4jMetaProperty.META_PROPERTY_LABEL.name()))
                .flatMap(label -> StreamFactory.stream(GlobalGraphOperations.at(this.graph.getBaseGraph()).getAllNodesWithLabel(DynamicLabel.label(label)).iterator()))
                .map(node -> new Neo4jVertex(node, this.graph));
    }

    private Stream<Vertex> getVerticesUsingLegacyIndex(final String key, final Object value) {
        final AutoIndexer indexer = this.graph.getBaseGraph().index().getNodeAutoIndexer();
        return indexer.isEnabled() && indexer.getAutoIndexedProperties().contains(key) ?
                StreamFactory.stream(this.graph.getBaseGraph().index().getNodeAutoIndexer().getAutoIndex().get(key, value).iterator())
                        .map(node -> node.getLabels().iterator().next().equals(Neo4jMetaProperty.META_PROPERTY_LABEL) ?
                                node.getRelationships(Direction.INCOMING).iterator().next().getStartNode() :
                                node)
                        .map(node -> new Neo4jVertex(node, this.graph)) :
                Stream.empty();
    }

    private Stream<Neo4jEdge> getEdgesUsingIndex(final HasContainer indexedContainer) {
        final RelationshipAutoIndexer indexer = this.graph.getBaseGraph().index().getRelationshipAutoIndexer();
        return StreamFactory.stream(indexer.getAutoIndex().get(indexedContainer.key, indexedContainer.value).iterator())
                .filter(relationship -> !relationship.getType().name().startsWith(Neo4jMetaProperty.META_PROPERTY_PREFIX))
                .map(relationship -> new Neo4jEdge(relationship, this.graph));
    }

    private HasContainer getVertexIndexKey() {
        final Set<String> indexedKeys = this.graph.getBaseGraph().index().getNodeAutoIndexer().getAutoIndexedProperties();
        return this.hasContainers.stream()
                .filter(c -> (indexedKeys.contains(c.key) && c.predicate.equals(Compare.EQUAL)))
                .findFirst()
                .orElseGet(() -> null);
    }

    private HasContainer getEdgeIndexKey() {
        final Set<String> indexedKeys = this.graph.getBaseGraph().index().getRelationshipAutoIndexer().getAutoIndexedProperties();
        return this.hasContainers.stream()
                .filter(c -> (indexedKeys.contains(c.key) && c.predicate.equals(Compare.EQUAL)))
                .findFirst()
                .orElseGet(() -> null);
    }

    private String makeCypherQuery() {
        final StringBuilder builder = new StringBuilder("MATCH node WHERE ");
        int counter = 0;
        for (final HasContainer hasContainer : this.hasContainers) {
            if (hasContainer.hasLabel()) {
                if (counter++ > 0) builder.append(" AND ");
                builder.append("node:").append(hasContainer.label);
            }

            if (hasContainer.key.equals(Element.LABEL) && hasContainer.predicate.equals(Compare.EQUAL)) {
                if (counter++ > 0) builder.append(" AND ");
                builder.append("node:").append(hasContainer.value);
            } else {
                if (counter++ > 0) builder.append(" AND ");
                builder.append("node.").append(hasContainer.key).append(" ");
                if (hasContainer.predicate instanceof Compare) {
                    builder.append(((Compare) hasContainer.predicate).asString()).append(" ").append(toStringOfValue(hasContainer.value));
                } else if (hasContainer.predicate.equals(Contains.IN)) {
                    builder.append("IN [");
                    for (Object object : (Collection) hasContainer.value) {
                        builder.append(toStringOfValue(object)).append(",");
                    }
                    builder.replace(builder.length() - 1, builder.length(), "").append("]");
                }
            }

        }
        System.out.println(builder);
        return builder.toString();
    }

    private String toStringOfValue(final Object value) {
        if (value instanceof String)
            return "'" + value + "'";
        else return value.toString();
    }


}
