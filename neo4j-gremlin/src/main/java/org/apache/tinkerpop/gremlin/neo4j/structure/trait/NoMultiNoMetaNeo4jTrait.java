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
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jDirection;
import org.neo4j.tinkerpop.api.Neo4jNode;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class NoMultiNoMetaNeo4jTrait implements Neo4jTrait {

    private static final NoMultiNoMetaNeo4jTrait INSTANCE = new NoMultiNoMetaNeo4jTrait();

    private final static Predicate TRUE_PREDICATE = x -> true;

    public static NoMultiNoMetaNeo4jTrait instance() {
        return INSTANCE;
    }

    private NoMultiNoMetaNeo4jTrait() {

    }

    @Override
    public Predicate<Neo4jNode> getNodePredicate() {
        return TRUE_PREDICATE;
    }

    @Override
    public Predicate<Neo4jRelationship> getRelationshipPredicate() {
        return TRUE_PREDICATE;
    }

    @Override
    public void removeVertex(final Neo4jVertex vertex) {
        try {
            final Neo4jNode node = vertex.getBaseVertex();
            for (final Neo4jRelationship relationship : node.relationships(Neo4jDirection.BOTH)) {
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
        return vertex.getBaseVertex().hasProperty(key) ? new Neo4jVertexProperty<>(vertex, key, (V) vertex.getBaseVertex().getProperty(key)) : VertexProperty.<V>empty();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> getVertexProperties(final Neo4jVertex vertex, final String... keys) {
        return (Iterator) IteratorUtils.stream(vertex.getBaseVertex().getKeys())
                .filter(key -> ElementHelper.keyExists(key, keys))
                .map(key -> new Neo4jVertexProperty<>(vertex, key, (V) vertex.getBaseVertex().getProperty(key))).iterator();
    }

    @Override
    public <V> VertexProperty<V> setVertexProperty(final Neo4jVertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (cardinality != VertexProperty.Cardinality.single)
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        if (keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        try {
            vertex.getBaseVertex().setProperty(key, value);
            return new Neo4jVertexProperty<>(vertex, key, value);
        } catch (final IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value, iae);
        }
    }

    @Override
    public VertexProperty.Cardinality getCardinality(final String key) {
        return VertexProperty.Cardinality.single;
    }

    @Override
    public boolean supportsMultiProperties() {
        return false;
    }

    @Override
    public boolean supportsMetaProperties() {
        return false;
    }

    @Override
    public void removeVertexProperty(final Neo4jVertexProperty vertexProperty) {
        final Neo4jNode node = ((Neo4jVertex) vertexProperty.element()).getBaseVertex();
        if (node.hasProperty(vertexProperty.key()))
            node.removeProperty(vertexProperty.key());
    }

    @Override
    public <V> Property<V> setProperty(final Neo4jVertexProperty vertexProperty, final String key, final V value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <V> Property<V> getProperty(final Neo4jVertexProperty vertexProperty, final String key) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <V> Iterator<Property<V>> getProperties(final Neo4jVertexProperty vertexProperty, final String... keys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
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
                if (Compare.eq == hasContainer.getBiPredicate() && !hasContainer.getKey().equals(T.label.getAccessor())) {
                    if (graph.getBaseGraph().hasSchemaIndex(label.get(), hasContainer.getKey())) {
                        return IteratorUtils.stream(graph.getBaseGraph().findNodes(label.get(), hasContainer.getKey(), hasContainer.getValue()))
                                .map(node -> (Vertex) new Neo4jVertex(node, graph))
                                .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
                    }
                }
            }
            // find a vertex by label
            return IteratorUtils.stream(graph.getBaseGraph().findNodes(label.get()))
                    .map(node -> (Vertex) new Neo4jVertex(node, graph))
                    .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        } else {
            // linear scan
            return IteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll(vertex, hasContainers));
        }
    }
}
