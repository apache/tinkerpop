package com.tinkerpop.gremlin.neo4j.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Pieter Martin
 */
public class Neo4jGraphStep<E extends Element> extends GraphStep<E> {

    public final List<HasContainer> hasContainers = new ArrayList<>();

    public Neo4jGraphStep(final Traversal traversal, final Neo4jGraph graph, final Class<E> returnClass, final Object... ids) {
        super(traversal, graph, returnClass, ids);
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        this.graph.tx().readWrite();
        // ids are present, filter on them first
        if (this.ids != null && this.ids.length > 0)
            return IteratorUtils.filter(this.graph.iterators().edgeIterator(this.ids), edge -> HasContainer.testAll((Edge) edge, this.hasContainers));
        final HasContainer hasContainer = this.getHasContainerForAutomaticIndex(Edge.class);
        return (null == hasContainer) ?
                IteratorUtils.filter(this.graph.iterators().edgeIterator(), edge -> HasContainer.testAll((Edge) edge, this.hasContainers)) :
                getEdgesUsingAutomaticIndex(hasContainer).filter(edge -> HasContainer.testAll((Edge) edge, this.hasContainers)).iterator();
    }

    private Iterator<? extends Vertex> vertices() {
        this.graph.tx().readWrite();
        // ids are present, filter on them first
        if (this.ids != null && this.ids.length > 0)
            return IteratorUtils.filter(this.graph.iterators().vertexIterator(this.ids), vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers));
        // a label and a property
        final Pair<String, HasContainer> labelHasPair = this.getHasContainerForLabelIndex();
        if (null != labelHasPair)
            return this.getVerticesUsingLabelAndProperty(labelHasPair.getValue0(), labelHasPair.getValue1())
                    .filter(vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers)).iterator();
        // use automatic indices
        final HasContainer hasContainer = this.getHasContainerForAutomaticIndex(Vertex.class);
        if (null != hasContainer)
            return this.getVerticesUsingAutomaticIndex(hasContainer)
                    .filter(vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers)).iterator();
        // only labels
        final List<String> labels = this.getLabels();
        if (null != labels)
            return this.getVerticesUsingOnlyLabels(labels).filter(vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers)).iterator();
        // linear scan
        return IteratorUtils.filter(this.graph.iterators().vertexIterator(), vertex -> HasContainer.testAll((Vertex) vertex, this.hasContainers));
    }


    private Stream<Neo4jVertex> getVerticesUsingLabelAndProperty(final String label, final HasContainer hasContainer) {
        //System.out.println("labelProperty: " + label + ":" + hasContainer);
        final ResourceIterator<Node> iterator1 = this.getGraph(Neo4jGraph.class).getBaseGraph().findNodesByLabelAndProperty(DynamicLabel.label(label), hasContainer.key, hasContainer.value).iterator();
        final ResourceIterator<Node> iterator2 = this.getGraph(Neo4jGraph.class).getBaseGraph().findNodesByLabelAndProperty(DynamicLabel.label(hasContainer.key), T.value.getAccessor(), hasContainer.value).iterator();
        final Stream<Neo4jVertex> stream1 = StreamFactory.stream(iterator1)
                .filter(node -> ElementHelper.idExists(node.getId(), this.ids))
                .map(node -> new Neo4jVertex(node, this.getGraph(Neo4jGraph.class)));
        final Stream<Neo4jVertex> stream2 = StreamFactory.stream(iterator2)
                .filter(node -> ElementHelper.idExists(node.getId(), this.ids))
                .filter(node -> node.getProperty(T.key.getAccessor()).equals(hasContainer.key))
                .map(node -> node.getRelationships(Direction.INCOMING).iterator().next().getStartNode())
                .map(node -> new Neo4jVertex(node, this.getGraph(Neo4jGraph.class)));
        return Stream.concat(stream1, stream2);
    }

    private Stream<Neo4jVertex> getVerticesUsingOnlyLabels(final List<String> labels) {
        //System.out.println("labels: " + labels);
        return labels.stream()
                .filter(label -> !label.equals(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL.name()))
                .flatMap(label -> StreamFactory.stream(GlobalGraphOperations.at(this.getGraph(Neo4jGraph.class).getBaseGraph()).getAllNodesWithLabel(DynamicLabel.label(label)).iterator()))
                .filter(node -> !node.hasLabel(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL))
                .filter(node -> ElementHelper.idExists(node.getId(), this.ids))
                .map(node -> new Neo4jVertex(node, this.getGraph(Neo4jGraph.class)));
    }

    private Stream<Neo4jVertex> getVerticesUsingAutomaticIndex(final HasContainer hasContainer) {
        //System.out.println("automatic index: " + hasContainer);
        return StreamFactory.stream(this.getGraph(Neo4jGraph.class).getBaseGraph().index().getNodeAutoIndexer().getAutoIndex().get(hasContainer.key, hasContainer.value).iterator())
                .map(node -> node.hasLabel(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL) ?
                        node.getRelationships(Direction.INCOMING).iterator().next().getStartNode() :
                        node)
                .filter(node -> ElementHelper.idExists(node.getId(), this.ids))
                .map(node -> new Neo4jVertex(node, this.getGraph(Neo4jGraph.class)));
    }

    private Stream<Neo4jEdge> getEdgesUsingAutomaticIndex(final HasContainer hasContainer) {
        return StreamFactory.stream(this.getGraph(Neo4jGraph.class).getBaseGraph().index().getRelationshipAutoIndexer().getAutoIndex().get(hasContainer.key, hasContainer.value).iterator())
                .filter(relationship -> ElementHelper.idExists(relationship.getId(), this.ids))
                .filter(relationship -> !relationship.getType().name().startsWith(Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX))
                .map(relationship -> new Neo4jEdge(relationship, this.getGraph(Neo4jGraph.class)));
    }

    private Pair<String, HasContainer> getHasContainerForLabelIndex() {
        for (final HasContainer hasContainer : this.hasContainers) {
            if (hasContainer.key.equals(T.label.getAccessor()) && hasContainer.predicate.equals(Compare.eq)) {
                for (final IndexDefinition index : this.getGraph(Neo4jGraph.class).getBaseGraph().schema().getIndexes(DynamicLabel.label((String) hasContainer.value))) {
                    for (final HasContainer hasContainer1 : this.hasContainers) {
                        if (!hasContainer1.key.equals(T.label.getAccessor()) && hasContainer1.predicate.equals(Compare.eq)) {
                            for (final String key : index.getPropertyKeys()) {
                                if (key.equals(hasContainer1.key))
                                    return Pair.with((String) hasContainer.value, hasContainer1);
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    private List<String> getLabels() {
        for (final HasContainer hasContainer : this.hasContainers) {
            if (hasContainer.key.equals(T.label.getAccessor()) && hasContainer.predicate.equals(Compare.eq))
                return Arrays.asList(((String) hasContainer.value));
            else if (hasContainer.key.equals(T.label.getAccessor()) && hasContainer.predicate.equals(Contains.within))
                return new ArrayList<>((Collection<String>) hasContainer.value);
        }
        return null;
    }

    private HasContainer getHasContainerForAutomaticIndex(final Class<? extends Element> elementClass) {
        final AutoIndexer<?> indexer = elementClass.equals(Vertex.class) ?
                this.getGraph(Neo4jGraph.class).getBaseGraph().index().getNodeAutoIndexer() :
                this.getGraph(Neo4jGraph.class).getBaseGraph().index().getRelationshipAutoIndexer();

        if (!indexer.isEnabled())
            return null;
        final Set<String> indexKeys = indexer.getAutoIndexedProperties();
        for (final HasContainer hasContainer : this.hasContainers) {
            if (hasContainer.predicate.equals(Compare.eq) && indexKeys.contains(hasContainer.key))
                return hasContainer;
        }
        return null;
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    TraversalHelper.makeStepString(this, this.hasContainers) :
                    TraversalHelper.makeStepString(this, Arrays.toString(this.ids), this.hasContainers);
    }

    /*private String makeCypherQuery() {
        final StringBuilder builder = new StringBuilder("MATCH node WHERE ");
        int counter = 0;
        for (final HasContainer hasContainer : this.hasContainers) {
            if (hasContainer.key.equals(T.label.getAccessor()) && hasContainer.predicate.equals(Compare.EQUAL)) {
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
    }*/
}
