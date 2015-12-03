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
package org.apache.tinkerpop.gremlin.neo4j.structure.trait;

import org.apache.tinkerpop.gremlin.neo4j.process.traversal.LabelP;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jHelper;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jProperty;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
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
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MultiMetaNeo4jTrait implements Neo4jTrait {

    private static final MultiMetaNeo4jTrait INSTANCE = new MultiMetaNeo4jTrait();

    public static final String VERTEX_PROPERTY_LABEL = "vertexProperty";
    public static final String VERTEX_PROPERTY_TOKEN = Graph.Hidden.hide("vertexProperty");

    private static final Predicate<Neo4jNode> NODE_PREDICATE = node -> !node.hasLabel(VERTEX_PROPERTY_LABEL);
    private static final Predicate<Neo4jRelationship> RELATIONSHIP_PREDICATE = relationship -> !Graph.Hidden.isHidden(relationship.type());

    private MultiMetaNeo4jTrait() {

    }

    public static MultiMetaNeo4jTrait instance() {
        return INSTANCE;
    }

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
            for (final Neo4jRelationship relationship : node.relationships(Neo4jDirection.BOTH)) {
                final Neo4jNode otherNode = relationship.other(node);
                if (otherNode.hasLabel(VERTEX_PROPERTY_LABEL)) {
                    otherNode.delete(); // meta property node
                }
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
                if (node.degree(Neo4jDirection.OUTGOING, Graph.Hidden.hide(key)) > 1)
                    throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
                else {
                    return (VertexProperty<V>) new Neo4jVertexProperty<>(vertex, node.relationships(Neo4jDirection.OUTGOING, Graph.Hidden.hide(key)).iterator().next().end());
                }
            } else {
                return new Neo4jVertexProperty<>(vertex, key, (V) node.getProperty(key));
            }
        } else
            return VertexProperty.<V>empty();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> getVertexProperties(final Neo4jVertex vertex, final String... keys) {
        if (Neo4jHelper.isDeleted(vertex.getBaseVertex()))
            return Collections.emptyIterator(); // TODO: I believe its because the vertex property is deleted, but then seen again in the iterator. ?
        return IteratorUtils.stream(vertex.getBaseVertex().getKeys())
                .filter(key -> ElementHelper.keyExists(key, keys))
                .flatMap(key -> {
                    if (vertex.getBaseVertex().getProperty(key).equals(VERTEX_PROPERTY_TOKEN))
                        return IteratorUtils.stream(vertex.getBaseVertex().relationships(Neo4jDirection.OUTGOING, Graph.Hidden.hide(key)))
                                .map(relationship -> (VertexProperty<V>) new Neo4jVertexProperty<>(vertex, relationship.end()));
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
            final String prefixedKey = Graph.Hidden.hide(key);
            if (node.hasProperty(key)) {
                if (node.getProperty(key).equals(VERTEX_PROPERTY_TOKEN)) {
                    final Neo4jNode vertexPropertyNode = graph.createNode(VERTEX_PROPERTY_LABEL, key);
                    vertexPropertyNode.setProperty(T.key.getAccessor(), key);
                    vertexPropertyNode.setProperty(T.value.getAccessor(), value);
                    vertexPropertyNode.setProperty(key, value);
                    node.connectTo(vertexPropertyNode, prefixedKey);
                    final Neo4jVertexProperty<V> property = new Neo4jVertexProperty<>(vertex, key, value, vertexPropertyNode);
                    ElementHelper.attachProperties(property, keyValues); // TODO: make this inlined
                    return property;
                } else {
                    // move current key to be a vertex property node
                    Neo4jNode vertexPropertyNode = graph.createNode(VERTEX_PROPERTY_LABEL, key);
                    final Object tempValue = node.removeProperty(key);
                    vertexPropertyNode.setProperty(T.key.getAccessor(), key);
                    vertexPropertyNode.setProperty(T.value.getAccessor(), tempValue);
                    vertexPropertyNode.setProperty(key, tempValue);
                    node.connectTo(vertexPropertyNode, prefixedKey);
                    node.setProperty(key, VERTEX_PROPERTY_TOKEN);
                    vertexPropertyNode = graph.createNode(VERTEX_PROPERTY_LABEL, key);
                    vertexPropertyNode.setProperty(T.key.getAccessor(), key);
                    vertexPropertyNode.setProperty(T.value.getAccessor(), value);
                    vertexPropertyNode.setProperty(key, value);
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
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value, iae);
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
            if (vertexNode.degree(Neo4jDirection.OUTGOING, Graph.Hidden.hide(vertexProperty.key())) == 0) {
                if (vertexNode.hasProperty(vertexProperty.key()))
                    vertexNode.removeProperty(vertexProperty.key());
            }
        } else {
            vertexPropertyNode.relationships(Neo4jDirection.BOTH).forEach(Neo4jRelationship::delete);
            vertexPropertyNode.delete();
            if (vertexNode.degree(Neo4jDirection.OUTGOING, Graph.Hidden.hide(vertexProperty.key())) == 0) {
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
            newVertexPropertyNode.setProperty(vertexProperty.key(), vertexProperty.value());
            newVertexPropertyNode.setProperty(key, value);
            vertexNode.connectTo(newVertexPropertyNode, Graph.Hidden.hide(vertexProperty.key()));
            vertexNode.setProperty(vertexProperty.key(), VERTEX_PROPERTY_TOKEN);
            Neo4jHelper.setVertexPropertyNode(vertexProperty, newVertexPropertyNode);
            return new Neo4jProperty<>(vertexProperty, key, value);
        }
    }

    @Override
    public <V> Property<V> getProperty(final Neo4jVertexProperty vertexProperty, final String key) {
        final Neo4jNode vertexPropertyNode = Neo4jHelper.getVertexPropertyNode(vertexProperty);
        if (null != vertexPropertyNode && vertexPropertyNode.hasProperty(key))
            return new Neo4jProperty<>(vertexProperty, key, (V) vertexPropertyNode.getProperty(key));
        else
            return Property.empty();
    }

    @Override
    public <V> Iterator<Property<V>> getProperties(final Neo4jVertexProperty vertexProperty, final String... keys) {
        final Neo4jNode vertexPropertyNode = Neo4jHelper.getVertexPropertyNode(vertexProperty);
        if (null == vertexPropertyNode)
            return Collections.emptyIterator();
        else
            return IteratorUtils.stream(vertexPropertyNode.getKeys())
                    .filter(key -> ElementHelper.keyExists(key, keys))
                    .filter(key -> !key.equals(vertexProperty.key()))
                    .map(key -> (Property<V>) new Neo4jProperty<>(vertexProperty, key, (V) vertexPropertyNode.getProperty(key))).iterator();

    }

    @Override
    public Iterator<Vertex> lookupVertices(final Neo4jGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
        // ids are present, filter on them first
        if (ids.length > 0)
            return IteratorUtils.filter(graph.vertices(ids), vertex -> HasContainer.testAll(vertex, hasContainers));
        ////// do index lookups //////
        graph.tx().readWrite();
        // get a label being search on
        Optional<String> label = hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
                .map(hasContainer -> (String) hasContainer.getValue())
                .findAny();
        if (!label.isPresent())
            label = hasContainers.stream()
                    .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                    .filter(hasContainer -> hasContainer.getPredicate() instanceof LabelP)
                    .map(hasContainer -> (String) hasContainer.getValue())
                    .findAny();

        if (label.isPresent()) {
            // find a vertex by label and key/value
            for (final HasContainer hasContainer : hasContainers) {
                if (Compare.eq == hasContainer.getBiPredicate()) {
                    if (graph.getBaseGraph().hasSchemaIndex(label.get(), hasContainer.getKey())) {
                        return Stream.concat(
                                IteratorUtils.stream(graph.getBaseGraph().findNodes(label.get(), hasContainer.getKey(), hasContainer.getValue()))
                                        .filter(getNodePredicate())
                                        .map(node -> (Vertex) new Neo4jVertex(node, graph))
                                        .filter(vertex -> HasContainer.testAll(vertex, hasContainers)),
                                IteratorUtils.stream(graph.getBaseGraph().findNodes(VERTEX_PROPERTY_LABEL, hasContainer.getKey(), hasContainer.getValue()))  // look up indexed vertex property nodes
                                        .map(node -> node.relationships(Neo4jDirection.INCOMING).iterator().next().start())
                                        .map(node -> (Vertex) new Neo4jVertex(node, graph))
                                        .filter(vertex -> HasContainer.testAll(vertex, hasContainers))).iterator();
                    }
                }
            }
            // find a vertex by label
            return IteratorUtils.stream(graph.getBaseGraph().findNodes(label.get()))
                    .filter(getNodePredicate())
                    .map(node -> (Vertex) new Neo4jVertex(node, graph))
                    .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        } else {
            // linear scan
            return IteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll(vertex, hasContainers));
        }
    }
}
