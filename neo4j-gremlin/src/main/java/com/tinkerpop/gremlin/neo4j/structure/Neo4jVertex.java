package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jVertexTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.util.Neo4jGraphTraversal;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jVertex extends Neo4jElement implements Vertex, Vertex.Iterators, WrappedVertex<Node>, Neo4jVertexTraversal {

    public Neo4jVertex(final Node node, final Neo4jGraph graph) {
        super(graph);
        this.baseElement = node;
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        this.graph.tx().readWrite();
        if (!this.graph.supportsMultiProperties) {
            return existsInNeo4j(key) ? new Neo4jVertexProperty<V>(this, key, (V) this.getBaseVertex().getProperty(key)) : VertexProperty.<V>empty();
        } else {
            if (existsInNeo4j(key)) {
                if (this.getBaseVertex().getProperty(key).equals(Neo4jVertexProperty.VERTEX_PROPERTY_TOKEN)) {
                    if (this.getBaseVertex().getDegree(DynamicRelationshipType.withName(Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key)), org.neo4j.graphdb.Direction.OUTGOING) > 1)
                        throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
                    else
                        return new Neo4jVertexProperty<>(this, this.getBaseVertex().getRelationships(org.neo4j.graphdb.Direction.OUTGOING, DynamicRelationshipType.withName(Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key))).iterator().next().getEndNode());
                } else {
                    return new Neo4jVertexProperty<>(this, key, (V) this.getBaseVertex().getProperty(key));
                }
            } else
                return VertexProperty.<V>empty();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();
        try {
            if (!this.graph.supportsMultiProperties) {
                this.getBaseVertex().setProperty(key, value);
                return new Neo4jVertexProperty<>(this, key, value);
            } else {
                final String prefixedKey = Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key);
                if (this.getBaseVertex().hasProperty(key)) {
                    if (this.getBaseVertex().getProperty(key).equals(Neo4jVertexProperty.VERTEX_PROPERTY_TOKEN)) {
                        final Node node = this.graph.getBaseGraph().createNode(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL, DynamicLabel.label(Graph.Key.unHide(key)));
                        node.setProperty(T.key.getAccessor(), key);
                        node.setProperty(T.value.getAccessor(), value);
                        this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
                        return new Neo4jVertexProperty<>(this, node);
                    } else {
                        Node node = this.graph.getBaseGraph().createNode(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL, DynamicLabel.label(Graph.Key.unHide(key)));
                        node.setProperty(T.key.getAccessor(), key);
                        node.setProperty(T.value.getAccessor(), this.getBaseVertex().removeProperty(key));
                        this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
                        this.getBaseVertex().setProperty(key, Neo4jVertexProperty.VERTEX_PROPERTY_TOKEN);
                        node = this.graph.getBaseGraph().createNode(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL, DynamicLabel.label(Graph.Key.unHide(key)));
                        node.setProperty(T.key.getAccessor(), key);
                        node.setProperty(T.value.getAccessor(), value);
                        this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
                        return new Neo4jVertexProperty<>(this, node);
                    }
                } else {
                    this.getBaseVertex().setProperty(key, value);
                    return new Neo4jVertexProperty<>(this, key, value);
                }
            }
        } catch (IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
    }

    @Override
    public <V> VertexProperty<V> singleProperty(final String key, final V value, final Object... keyValues) {
        if (!this.graph.supportsMultiProperties) {
            this.getBaseVertex().setProperty(key, value);
            return new Neo4jVertexProperty<>(this, key, value);
        } else {
            ElementHelper.legalPropertyKeyValueArray(keyValues);
            this.graph.tx().readWrite();
            final String prefixedKey = Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key);
            this.getBaseVertex().getRelationships(org.neo4j.graphdb.Direction.OUTGOING, DynamicRelationshipType.withName(prefixedKey)).forEach(relationship -> {
                final Node multiPropertyNode = relationship.getEndNode();
                relationship.delete();
                multiPropertyNode.delete();
            });
            if (keyValues.length == 0) {
                this.getBaseVertex().setProperty(key, value);
                return new Neo4jVertexProperty<>(this, key, value);
            } else {
                this.getBaseVertex().setProperty(key, Neo4jVertexProperty.VERTEX_PROPERTY_TOKEN);
                final Node node = this.graph.getBaseGraph().createNode(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL, DynamicLabel.label(Graph.Key.unHide(key)));
                node.setProperty(T.key.getAccessor(), key);
                node.setProperty(T.value.getAccessor(), value);
                for (int i = 0; i < keyValues.length; i = i + 2) {
                    node.setProperty((String) keyValues[i], keyValues[i + 1]);
                }
                this.getBaseVertex().createRelationshipTo(node, DynamicRelationshipType.withName(prefixedKey));
                return new Neo4jVertexProperty<>(this, node);
            }
        }
    }

    @Override
    public void remove() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        this.removed = true;
        this.graph.tx().readWrite();
        try {
            final Node node = this.getBaseVertex();
            for (final Relationship relationship : node.getRelationships(org.neo4j.graphdb.Direction.BOTH)) {
                final Node otherNode = relationship.getOtherNode(node);
                if (otherNode.hasLabel(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL)) {
                    otherNode.getRelationships().forEach(Relationship::delete);
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
        ElementHelper.validateLabel(label);
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
        final Neo4jTraversal<Vertex, Vertex> traversal = new Neo4jGraphTraversal<>(this.graph);
        return (Neo4jTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    @Override
    public Node getBaseVertex() {
        return (Node) this.baseElement;
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return this.getBaseVertex().getLabels().iterator().next().name();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... labels) {
        graph.tx().readWrite();
        return (Iterator) Neo4jHelper.getVertices(Neo4jVertex.this, direction, labels).iterator();
    }

    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        graph.tx().readWrite();
        return (Iterator) Neo4jHelper.getEdges(Neo4jVertex.this, direction, edgeLabels).iterator();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        graph.tx().readWrite();
        return (Iterator) StreamFactory.stream(getBaseVertex().getPropertyKeys())
                .filter(key -> propertyKeys.length == 0 || Stream.of(propertyKeys).filter(k -> k.equals(key)).findAny().isPresent())
                .filter(key -> !Graph.Key.isHidden(key))
                .flatMap(key -> {
                    if (getBaseVertex().getProperty(key).equals(Neo4jVertexProperty.VERTEX_PROPERTY_TOKEN))
                        return StreamFactory.stream(getBaseVertex().getRelationships(org.neo4j.graphdb.Direction.OUTGOING, DynamicRelationshipType.withName(Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key))))
                                .map(relationship -> new Neo4jVertexProperty(Neo4jVertex.this, relationship.getEndNode()));
                    else
                        return Stream.of(new Neo4jVertexProperty<>(Neo4jVertex.this, key, (V) getBaseVertex().getProperty(key)));
                }).iterator();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> hiddenPropertyIterator(final String... propertyKeys) {
        graph.tx().readWrite();
        return (Iterator) StreamFactory.stream(getBaseVertex().getPropertyKeys())
                .filter(Graph.Key::isHidden)
                .filter(key -> propertyKeys.length == 0 || Stream.of(propertyKeys).filter(k -> k.equals(Graph.Key.unHide(key))).findAny().isPresent())
                .flatMap(key -> {
                    if (getBaseVertex().getProperty(key).equals(Neo4jVertexProperty.VERTEX_PROPERTY_TOKEN))
                        return StreamFactory.stream(getBaseVertex().getRelationships(org.neo4j.graphdb.Direction.OUTGOING, DynamicRelationshipType.withName(Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key))))
                                .map(relationship -> new Neo4jVertexProperty(Neo4jVertex.this, relationship.getEndNode()));
                    else
                        return Stream.of(new Neo4jVertexProperty<>(Neo4jVertex.this, key, (V) getBaseVertex().getProperty(key)));
                }).iterator();
    }

    private boolean existsInNeo4j(final String key) {
        try {
            return this.getBaseVertex().hasProperty(key);
        } catch (IllegalStateException | NotFoundException ex) {
            // if vertex is removed before/after transaction close
            throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id());
        }
    }
}
