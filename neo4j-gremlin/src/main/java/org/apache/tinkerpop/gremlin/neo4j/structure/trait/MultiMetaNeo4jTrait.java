/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.neo4j.structure.trait;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jHelper;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jProperty;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jDirection;
import org.neo4j.tinkerpop.api.Neo4jGraphAPI;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MultiMetaNeo4jTrait implements Neo4jTrait {

    public static final String VERTEX_PROPERTY_LABEL = "vertexProperty";
    public static final String VERTEX_PROPERTY_PREFIX = Graph.Hidden.hide("");
    public static final String VERTEX_PROPERTY_TOKEN = Graph.Hidden.hide("vertexProperty");

    private static final Predicate<Neo4jNode> NODE_PREDICATE = node -> !node.hasLabel(VERTEX_PROPERTY_LABEL);
    private static final Predicate<Neo4jRelationship> RELATIONSHIP_PREDICATE = relationship -> !relationship.type().startsWith(VERTEX_PROPERTY_PREFIX);

    @Override
    public Predicate<Neo4jNode> getNodePredicate() {
        return NODE_PREDICATE;
    }

    @Override
    public Predicate<Neo4jRelationship> getRelationshipPredicate() {
        return RELATIONSHIP_PREDICATE;
    }

    @Override
    public void removeVertex(final Neo4jVertex vertex) {
        try {
            final Neo4jNode node = vertex.getBaseVertex();
            for (final Neo4jRelationship relationship : node.relationships(Neo4jDirection.OUTGOING)) {
                final Neo4jNode otherNode = relationship.other(node);
                if (otherNode.hasLabel(VERTEX_PROPERTY_LABEL)) {
                    otherNode.relationships(Neo4jDirection.BOTH).forEach(Neo4jRelationship::delete);
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
    public <V> VertexProperty<V> getVertexProperty(final Neo4jVertex vertex, final String key) {
        final Neo4jNode node = vertex.getBaseVertex();
        if (node.hasProperty(key)) {
            if (node.getProperty(key).equals(VERTEX_PROPERTY_TOKEN)) {
                if (node.degree(Neo4jDirection.OUTGOING, VERTEX_PROPERTY_PREFIX.concat(key)) > 1)
                    throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
                else {
                    final Neo4jNode vertexPropertyNode = node.relationships(Neo4jDirection.OUTGOING, VERTEX_PROPERTY_PREFIX.concat(key)).iterator().next().end();
                    return new Neo4jVertexProperty<>(vertex, (String) vertexPropertyNode.getProperty(T.key.getAccessor()), (V) vertexPropertyNode.getProperty(T.value.getAccessor()), vertexPropertyNode);
                }
            } else {
                return new Neo4jVertexProperty<>(vertex, key, (V) node.getProperty(key));
            }
        } else
            return VertexProperty.<V>empty();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> getVertexProperties(final Neo4jVertex vertex, final String... keys) {
        if (Neo4jHelper.isDeleted(vertex.getBaseVertex())) return Collections.emptyIterator(); // TODO: WHY?
        return IteratorUtils.stream(vertex.getBaseVertex().getKeys())
                .filter(key -> ElementHelper.keyExists(key, keys))
                .flatMap(key -> {
                    if (vertex.getBaseVertex().getProperty(key).equals(VERTEX_PROPERTY_TOKEN))
                        return IteratorUtils.stream(vertex.getBaseVertex().relationships(Neo4jDirection.OUTGOING, (VERTEX_PROPERTY_PREFIX.concat(key))))
                                .map(relationship -> {
                                    final Neo4jNode vertexPropertyNode = relationship.end();
                                    return (VertexProperty<V>) new Neo4jVertexProperty<>(vertex, (String) vertexPropertyNode.getProperty(T.key.getAccessor()), vertexPropertyNode.getProperty(T.value.getAccessor()), vertexPropertyNode);
                                });
                    else
                        return Stream.of(new Neo4jVertexProperty<>(vertex, key, (V) vertex.getBaseVertex().getProperty(key)));
                }).iterator();
    }

    @Override
    public <V> VertexProperty<V> setVertexProperty(Neo4jVertex vertex, VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        try {
            final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(vertex, cardinality, key, value, keyValues);
            if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();
            final Neo4jNode node = vertex.getBaseVertex();
            final Neo4jGraphAPI graph = ((Neo4jGraph) vertex.graph()).getBaseGraph();
            final String prefixedKey = VERTEX_PROPERTY_PREFIX.concat(key);
            if (node.hasProperty(key)) {
                if (node.getProperty(key).equals(VERTEX_PROPERTY_TOKEN)) {
                    final Neo4jNode vertexPropertyNode = graph.createNode(VERTEX_PROPERTY_LABEL, key);
                    vertexPropertyNode.setProperty(T.key.getAccessor(), key);
                    vertexPropertyNode.setProperty(T.value.getAccessor(), value);
                    node.connectTo(vertexPropertyNode, prefixedKey);
                    final Neo4jVertexProperty<V> property = new Neo4jVertexProperty<>(vertex, key, value, vertexPropertyNode);
                    ElementHelper.attachProperties(property, keyValues); // TODO: make this inlined
                    return property;
                } else {
                    Neo4jNode vertexPropertyNode = graph.createNode(VERTEX_PROPERTY_LABEL, key);
                    vertexPropertyNode.setProperty(T.key.getAccessor(), key);
                    vertexPropertyNode.setProperty(T.value.getAccessor(), node.removeProperty(key));
                    node.connectTo(vertexPropertyNode, prefixedKey);
                    node.setProperty(key, VERTEX_PROPERTY_TOKEN);
                    vertexPropertyNode = graph.createNode(VERTEX_PROPERTY_LABEL, key);
                    vertexPropertyNode.setProperty(T.key.getAccessor(), key);
                    vertexPropertyNode.setProperty(T.value.getAccessor(), value);
                    node.connectTo(vertexPropertyNode, prefixedKey);
                    final Neo4jVertexProperty<V> property = new Neo4jVertexProperty<>(vertex, key, value, vertexPropertyNode);
                    ElementHelper.attachProperties(property, keyValues); // TODO: make this inlined
                    return property;
                }
            } else {
                node.setProperty(key, value);
                final Neo4jVertexProperty<V> property = new Neo4jVertexProperty<>(vertex, key, value);
                ElementHelper.attachProperties(property, keyValues); // TODO: make this inlined
                return property;
            }
        } catch (final IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
    }

    @Override
    public VertexProperty.Cardinality getCardinality(final String key) {
        return VertexProperty.Cardinality.list;
    }

    @Override
    public boolean supportsMultiProperties() {
        return true;
    }

    @Override
    public boolean supportsMetaProperties() {
        return true;
    }

    @Override
    public void removeVertexProperty(final Neo4jVertexProperty vertexProperty) {
        final Neo4jNode vertexPropertyNode = Neo4jHelper.getVertexPropertyNode(vertexProperty);
        final Neo4jNode vertexNode = ((Neo4jVertex) vertexProperty.element()).getBaseVertex();
        if (null == vertexPropertyNode) {
            if (vertexNode.degree(Neo4jDirection.OUTGOING, VERTEX_PROPERTY_PREFIX.concat(vertexProperty.key())) == 0) {
                if (vertexNode.hasProperty(vertexProperty.key()))
                    vertexNode.removeProperty(vertexProperty.key());
            }
        } else {
            vertexPropertyNode.relationships(null).forEach(Neo4jRelationship::delete);
            vertexPropertyNode.delete();
            if (vertexNode.degree(Neo4jDirection.OUTGOING, VERTEX_PROPERTY_PREFIX.concat(vertexProperty.key())) == 0) {
                if (vertexNode.hasProperty(vertexProperty.key()))
                    vertexNode.removeProperty(vertexProperty.key());
            }
        }
    }

    @Override
    public <V> Property<V> setProperty(final Neo4jVertexProperty vertexProperty, final String key, final V value) {
        final Neo4jNode vertexPropertyNode = Neo4jHelper.getVertexPropertyNode(vertexProperty);
        if (null != vertexPropertyNode) {
            vertexPropertyNode.setProperty(key, value);
            return new Neo4jProperty<>(vertexProperty, key, value);
        } else {
            final Neo4jNode vertexNode = ((Neo4jVertex) vertexProperty.element()).getBaseVertex();
            final Neo4jNode newVertexPropertyNode = ((WrappedGraph<Neo4jGraphAPI>) vertexProperty.element().graph()).getBaseGraph().createNode(VERTEX_PROPERTY_LABEL, vertexProperty.label());
            newVertexPropertyNode.setProperty(T.key.getAccessor(), vertexProperty.key());
            newVertexPropertyNode.setProperty(T.value.getAccessor(), vertexProperty.value());
            newVertexPropertyNode.setProperty(key, value);
            vertexNode.connectTo(newVertexPropertyNode, VERTEX_PROPERTY_PREFIX.concat(vertexProperty.key()));
            vertexNode.setProperty(vertexProperty.key(), VERTEX_PROPERTY_TOKEN);
            Neo4jHelper.setVertexPropertyNode(vertexProperty, newVertexPropertyNode);
            return new Neo4jProperty<>(vertexProperty, key, value);
        }
    }

    @Override
    public <V> Property<V> getProperty(final Neo4jVertexProperty vertexProperty, final String key) {
        try {
            final Neo4jNode vertexPropertyNode = Neo4jHelper.getVertexPropertyNode(vertexProperty);
            if (null != vertexPropertyNode && vertexPropertyNode.hasProperty(key))
                return new Neo4jProperty<>(vertexProperty, key, (V) vertexPropertyNode.getProperty(key));
            else
                return Property.empty();
        } catch (IllegalStateException ex) {
            throw Element.Exceptions.elementAlreadyRemoved(vertexProperty.getClass(), vertexProperty.id());
        } catch (RuntimeException ex) {
            if (Neo4jHelper.isNotFound(ex))
                throw Element.Exceptions.elementAlreadyRemoved(vertexProperty.getClass(), vertexProperty.id());
            throw ex;
        }
    }

    @Override
    public <V> Iterator<Property<V>> getProperties(Neo4jVertexProperty vertexProperty, String... keys) {
        final Neo4jNode vertexPropertyNode = Neo4jHelper.getVertexPropertyNode(vertexProperty);
        if (null == vertexPropertyNode)
            return Collections.emptyIterator();
        else
            return IteratorUtils.map(IteratorUtils
                            .filter(vertexPropertyNode.getKeys().iterator(), key -> !key.equals(T.key.getAccessor()) && !key.equals(T.value.getAccessor()) && ElementHelper.keyExists(key, keys)),
                    key -> (Property<V>) new Neo4jProperty<>(vertexProperty, key, (V) vertexPropertyNode.getProperty(key)));
    }

    /*
     @Override
    public Set<String> keys() {
        if (isNode()) {
            this.vertex.graph().tx().readWrite();
            final Set<String> keys = new HashSet<>();
            for (final String key : this.node.getKeys()) {
                if (!Graph.Hidden.isHidden(key))
                    keys.add(key);
            }
            return keys;
        } else {
            return Collections.emptySet();
        }
    }
     */
}
