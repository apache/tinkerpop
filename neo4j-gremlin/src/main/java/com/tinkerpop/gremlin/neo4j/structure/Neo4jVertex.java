package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jVertexTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.util.DefaultNeo4jTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jVertex extends Neo4jElement implements Vertex, WrappedVertex<Node>, Neo4jVertexTraversal {

    public Neo4jVertex(final Node node, final Neo4jGraph graph) {
        super(graph);
        this.baseElement = node;
    }

    @Override
    public <V> MetaProperty<V> property(final String key) {
        this.graph.tx().readWrite();
        if (this.getBaseVertex().hasProperty(key)) {
            if (this.getBaseVertex().getProperty(key).equals(Neo4jMetaProperty.META_PROPERTY_TOKEN)) {
                if (this.getBaseVertex().getDegree(DynamicRelationshipType.withName(Neo4jMetaProperty.META_PROPERTY_PREFIX.concat(key)), org.neo4j.graphdb.Direction.OUTGOING) > 1)
                    throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
                else
                    return new Neo4jMetaProperty<>(this, this.getBaseVertex().getRelationships(org.neo4j.graphdb.Direction.OUTGOING, DynamicRelationshipType.withName(Neo4jMetaProperty.META_PROPERTY_PREFIX.concat(key))).iterator().next().getEndNode());
            } else {
                return new Neo4jMetaProperty<>(this, key, (V) this.baseElement.getProperty(key));
            }
        } else
            return MetaProperty.<V>empty();
    }

    @Override
    public <V> MetaProperty<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();
        try {
            final String prefixedKey = Neo4jMetaProperty.META_PROPERTY_PREFIX.concat(key);
            if (this.getBaseVertex().hasProperty(key)) {
                if (this.getBaseVertex().getProperty(key).equals(Neo4jMetaProperty.META_PROPERTY_TOKEN)) {
                    final Node node = this.graph.getBaseGraph().createNode(Neo4jMetaProperty.META_PROPERTY_LABEL);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_KEY, key);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_VALUE, value);
                    this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
                    return new Neo4jMetaProperty<>(this, node);
                } else {
                    Node node = this.graph.getBaseGraph().createNode(Neo4jMetaProperty.META_PROPERTY_LABEL);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_KEY, key);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_VALUE, this.getBaseVertex().removeProperty(key));
                    this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
                    this.getBaseVertex().setProperty(key, Neo4jMetaProperty.META_PROPERTY_TOKEN);
                    node = this.graph.getBaseGraph().createNode(Neo4jMetaProperty.META_PROPERTY_LABEL);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_KEY, key);
                    node.setProperty(Neo4jMetaProperty.META_PROPERTY_VALUE, value);
                    this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
                    return new Neo4jMetaProperty<>(this, node);
                }
            } else {
                this.getBaseVertex().setProperty(key, value);
                return new Neo4jMetaProperty<>(this, key, value);
            }
        } catch (IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
    }

    @Override
    // TODO: remove if MetaPropertyTest works with a setProperty() bug in Neo4j fixed.
    public <V> MetaProperty<V> singleProperty(final String key, final V value, final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        this.graph.tx().readWrite();
        final String prefixedKey = Neo4jMetaProperty.META_PROPERTY_PREFIX.concat(key);
        this.getBaseVertex().getRelationships(org.neo4j.graphdb.Direction.OUTGOING, DynamicRelationshipType.withName(prefixedKey)).forEach(relationship -> {
            final Node multiPropertyNode = relationship.getEndNode();
            relationship.delete();
            multiPropertyNode.delete();
        });
        if (keyValues.length == 0) {
            this.getBaseVertex().setProperty(key, value);
            return new Neo4jMetaProperty<>(this, key, value);
        } else {
            this.getBaseVertex().setProperty(key, Neo4jMetaProperty.META_PROPERTY_TOKEN);
            final Node node = this.graph.getBaseGraph().createNode(Neo4jMetaProperty.META_PROPERTY_LABEL);
            node.setProperty(Neo4jMetaProperty.META_PROPERTY_KEY, key);
            node.setProperty(Neo4jMetaProperty.META_PROPERTY_VALUE, value);
            for (int i = 0; i < keyValues.length; i = i + 2) {
                node.setProperty((String) keyValues[i], keyValues[i + 1]);
            }
            this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
            return new Neo4jMetaProperty<>(this, node);
        }
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        try {
            final Node node = this.getBaseVertex();
            for (final Relationship relationship : node.getRelationships(org.neo4j.graphdb.Direction.BOTH)) {
                final Node otherNode = relationship.getOtherNode(node);
                if (otherNode.getLabels().iterator().next().equals(Neo4jMetaProperty.META_PROPERTY_LABEL)) {
                    relationship.getOtherNode(node).getRelationships().forEach(Relationship::delete);
                    otherNode.delete(); // meta property node
                } else
                    relationship.delete();
            }
            node.delete();
        } catch (final NotFoundException ignored) {
            // this one happens if the vertex is committed
        } catch (final IllegalStateException ignored) {
            // this one happens if the vertex is still chilling in the tx
        }
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (label == null)
            throw Edge.Exceptions.edgeLabelCanNotBeNull();
        if (label.startsWith(Neo4jMetaProperty.META_PROPERTY_PREFIX))
            throw new IllegalArgumentException("The edge label prefix " + Neo4jMetaProperty.META_PROPERTY_PREFIX + " is reserved for Neo4jMetaProperties.");

        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();

        this.graph.tx().readWrite();
        final Node node = (Node) this.baseElement;
        final Neo4jEdge edge = new Neo4jEdge(node.createRelationshipTo(((Neo4jVertex) inVertex).getBaseVertex(),
                DynamicRelationshipType.withName(label)), this.graph);
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    @Override
    public Neo4jTraversal<Vertex, Vertex> start() {
        final Neo4jTraversal<Vertex, Vertex> traversal = new DefaultNeo4jTraversal<>(this.graph);
        return (Neo4jTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    @Override
    public Node getBaseVertex() {
        return (Node) this.baseElement;
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Vertex.Iterators iterators() {
        return this.iterators;
    }

    private final Vertex.Iterators iterators = new Iterators();

    protected class Iterators extends Neo4jElement.Iterators implements Vertex.Iterators {

        @Override
        public Iterator<Vertex> vertices(final Direction direction, final int branchFactor, final String... labels) {
            graph.tx().readWrite();
            return (Iterator) StreamFactory.stream(Neo4jHelper.getVertices(Neo4jVertex.this, direction, labels)).limit(branchFactor).iterator();
        }

        @Override
        public Iterator<Edge> edges(final Direction direction, final int branchFactor, final String... labels) {
            graph.tx().readWrite();
            return (Iterator) StreamFactory.stream(Neo4jHelper.getEdges(Neo4jVertex.this, direction, labels)).limit(branchFactor).iterator();
        }

        @Override
        public <V> Iterator<MetaProperty<V>> properties(final String... propertyKeys) {
            graph.tx().readWrite();
            return (Iterator) StreamFactory.stream(getBaseVertex().getPropertyKeys())
                    .filter(key -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, key) >= 0)
                    .filter(key -> !Graph.Key.isHidden(key))
                    .flatMap(key -> {
                        if (getBaseVertex().getProperty(key).equals(Neo4jMetaProperty.META_PROPERTY_TOKEN))
                            return StreamFactory.stream(getBaseVertex().getRelationships(org.neo4j.graphdb.Direction.OUTGOING, DynamicRelationshipType.withName(Neo4jMetaProperty.META_PROPERTY_PREFIX.concat(key))))
                                    .map(relationship -> new Neo4jMetaProperty(Neo4jVertex.this, relationship.getEndNode()));
                        else
                            return Arrays.asList(new Neo4jMetaProperty<>(Neo4jVertex.this, key, (V) getBaseVertex().getProperty(key))).stream();
                    }).iterator();
        }

        @Override
        public <V> Iterator<MetaProperty<V>> hiddens(final String... propertyKeys) {
            graph.tx().readWrite();
            return (Iterator) StreamFactory.stream(getBaseVertex().getPropertyKeys())
                    .filter(key -> Graph.Key.isHidden(key))
                    .filter(key -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, Graph.Key.unHide(key)) >= 0)
                    .flatMap(key -> {
                        if (getBaseVertex().getProperty(key).equals(Neo4jMetaProperty.META_PROPERTY_TOKEN))
                            return StreamFactory.stream(getBaseVertex().getRelationships(org.neo4j.graphdb.Direction.OUTGOING, DynamicRelationshipType.withName(Neo4jMetaProperty.META_PROPERTY_PREFIX.concat(key))))
                                    .map(relationship -> new Neo4jMetaProperty(Neo4jVertex.this, relationship.getEndNode()));
                        else
                            return Arrays.asList(new Neo4jMetaProperty<>(Neo4jVertex.this, key, (V) getBaseVertex().getProperty(key))).stream();
                    }).iterator();
        }
    }
}
