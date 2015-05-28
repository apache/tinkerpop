/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jDirection;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Neo4jVertex extends Neo4jElement implements Vertex, WrappedVertex<Neo4jNode> {

    protected static final String LABEL_DELIMINATOR = "::";

    public Neo4jVertex(final Neo4jNode node, final Neo4jGraph graph) {
        super(node, graph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        this.graph.tx().readWrite();
        if (!this.graph.supportsMultiProperties) {
            return existsInNeo4j(key) ? new Neo4jVertexProperty<V>(this, key, (V) this.getBaseVertex().getProperty(key)) : VertexProperty.<V>empty();
        } else {
            if (existsInNeo4j(key)) {
                if (this.getBaseVertex().getProperty(key).equals(Neo4jVertexProperty.VERTEX_PROPERTY_TOKEN)) {
                    if (this.getBaseVertex().degree(Neo4jDirection.OUTGOING, Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key)) > 1)
                        throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
                    else
                        return new Neo4jVertexProperty<>(this, this.getBaseVertex().relationships(
                                Neo4jDirection.OUTGOING,
                                Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key))
                                .iterator().next().end());
                } else {
                    return new Neo4jVertexProperty<>(this, key, (V) this.getBaseVertex().getProperty(key));
                }
            } else
                return VertexProperty.<V>empty();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.property(VertexProperty.Cardinality.single, key, value);
    }

    // TODO:?
    @Override
    public <V> VertexProperty<V> property(String key, V value, Object... keyValues) {
        return this.property(VertexProperty.Cardinality.single, key, value, keyValues);
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        ElementHelper.validateProperty(key, value);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();
        this.graph.tx().readWrite();
        try {
            if (!this.graph.supportsMultiProperties) {
                this.getBaseVertex().setProperty(key, value);
                return new Neo4jVertexProperty<>(this, key, value);
            } else {
                final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
                if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

                final String prefixedKey = Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key);
                if (this.getBaseVertex().hasProperty(key)) {
                    if (this.getBaseVertex().getProperty(key).equals(Neo4jVertexProperty.VERTEX_PROPERTY_TOKEN)) {
                        final Neo4jNode node = this.graph.getBaseGraph().createNode(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL, key);
                        node.setProperty(T.key.getAccessor(), key);
                        node.setProperty(T.value.getAccessor(), value);
                        this.getBaseVertex().connectTo(node, prefixedKey);
                        final Neo4jVertexProperty<V> property = new Neo4jVertexProperty<>(this, node);
                        ElementHelper.attachProperties(property, keyValues); // TODO: make this inlined
                        return property;
                    } else {
                        Neo4jNode node = this.graph.getBaseGraph().createNode(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL, key);
                        node.setProperty(T.key.getAccessor(), key);
                        node.setProperty(T.value.getAccessor(), this.getBaseVertex().removeProperty(key));
                        this.getBaseVertex().connectTo(node, prefixedKey);
                        this.getBaseVertex().setProperty(key, Neo4jVertexProperty.VERTEX_PROPERTY_TOKEN);
                        node = this.graph.getBaseGraph().createNode(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL, key);
                        node.setProperty(T.key.getAccessor(), key);
                        node.setProperty(T.value.getAccessor(), value);
                        this.getBaseVertex().connectTo(node, prefixedKey);
                        final Neo4jVertexProperty<V> property = new Neo4jVertexProperty<>(this, node);
                        ElementHelper.attachProperties(property, keyValues); // TODO: make this inlined
                        return property;
                    }
                } else {
                    this.getBaseVertex().setProperty(key, value);
                    final Neo4jVertexProperty<V> property = new Neo4jVertexProperty<>(this, key, value);
                    ElementHelper.attachProperties(property, keyValues); // TODO: make this inlined
                    return property;
                }
            }
        } catch (IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
    }

    @Override
    public void remove() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        this.removed = true;
        this.graph.tx().readWrite();
        try {
            final Neo4jNode node = this.getBaseVertex();
            for (final Neo4jRelationship relationship : node.relationships(Neo4jDirection.BOTH)) {
                final Neo4jNode otherNode = relationship.other(node);
                if (otherNode.hasLabel(Neo4jVertexProperty.VERTEX_PROPERTY_LABEL)) {
                    otherNode.relationships(null).forEach(Neo4jRelationship::delete);
                    otherNode.delete(); // meta property node
                } else
                    relationship.delete();
            }
            node.delete();
        } catch (final IllegalStateException ignored) {
            // this one happens if the vertex is still chilling in the tx
        } catch (final RuntimeException ex) {
            if (!Neo4jHelper.isNotFound(ex)) throw ex;
            // this one happens if the vertex is committed
        }
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.getBaseVertex().getId());
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();

        this.graph.tx().readWrite();
        final Neo4jNode node = (Neo4jNode) this.baseElement;
        final Neo4jEdge edge = new Neo4jEdge(node.connectTo(((Neo4jVertex) inVertex).getBaseVertex(),
                label), this.graph);
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    @Override
    public Neo4jNode getBaseVertex() {
        return (Neo4jNode) this.baseElement;
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return String.join(LABEL_DELIMINATOR, this.labels());
    }

    /////////////// Neo4jVertex Specific Methods for Multi-Label Support ///////////////
    public Set<String> labels() {
        this.graph.tx().readWrite();
        final Set<String> labels = new TreeSet<>(this.getBaseVertex().labels());
        return Collections.unmodifiableSet(labels);
    }

    public void addLabel(final String label) {
        this.graph.tx().readWrite();
        this.getBaseVertex().addLabel(label);
    }

    public void removeLabel(final String label) {
        this.graph.tx().readWrite();
        this.getBaseVertex().removeLabel(label);
    }
    //////////////////////////////////////////////////////////////////////////////////////

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        this.graph.tx().readWrite();
        return new Iterator<Vertex>() {
            final Iterator<Neo4jRelationship> relationshipIterator = IteratorUtils.filter(0 == edgeLabels.length ?
                    getBaseVertex().relationships(Neo4jHelper.mapDirection(direction)).iterator() :
                    getBaseVertex().relationships(Neo4jHelper.mapDirection(direction), (edgeLabels)).iterator(), r -> !r.type().startsWith(Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX));

            @Override
            public boolean hasNext() {
                return this.relationshipIterator.hasNext();
            }

            @Override
            public Neo4jVertex next() {
                return new Neo4jVertex(this.relationshipIterator.next().other(getBaseVertex()), graph);
            }
        };
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        this.graph.tx().readWrite();
        return new Iterator<Edge>() {
            final Iterator<Neo4jRelationship> relationshipIterator = IteratorUtils.filter(0 == edgeLabels.length ?
                    getBaseVertex().relationships(Neo4jHelper.mapDirection(direction)).iterator() :
                    getBaseVertex().relationships(Neo4jHelper.mapDirection(direction), (edgeLabels)).iterator(), r -> !r.type().startsWith(Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX));

            @Override
            public boolean hasNext() {
                return this.relationshipIterator.hasNext();
            }

            @Override
            public Neo4jEdge next() {
                return new Neo4jEdge(this.relationshipIterator.next(), graph);
            }
        };
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        this.graph.tx().readWrite();
        return IteratorUtils.stream(getBaseVertex().getKeys())
                .filter(key -> ElementHelper.keyExists(key, propertyKeys))
                .flatMap(key -> {
                    if (getBaseVertex().getProperty(key).equals(Neo4jVertexProperty.VERTEX_PROPERTY_TOKEN))
                        return IteratorUtils.stream(getBaseVertex().relationships(Neo4jDirection.OUTGOING, (Neo4jVertexProperty.VERTEX_PROPERTY_PREFIX.concat(key))))
                                .map(relationship -> (VertexProperty<V>) new Neo4jVertexProperty(Neo4jVertex.this, relationship.end()));
                    else
                        return Stream.of(new Neo4jVertexProperty<>(Neo4jVertex.this, key, (V) this.getBaseVertex().getProperty(key)));
                }).iterator();
    }

    private boolean existsInNeo4j(final String key) {
        try {
            return this.getBaseVertex().hasProperty(key);
        } catch (IllegalStateException ex) {
            // if vertex is removed before/after transaction close
            throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id());
        } catch (RuntimeException ex) {
            // if vertex is removed before/after transaction close
            if (Neo4jHelper.isNotFound(ex))
                throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id());
            throw ex;
        }
    }
}