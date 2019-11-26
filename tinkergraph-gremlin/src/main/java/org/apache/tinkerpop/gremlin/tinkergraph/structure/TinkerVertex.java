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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerVertex extends TinkerElement implements Vertex {

    protected Map<String, List<VertexProperty>> properties;
    protected Map<String, Set<Edge>> outEdges;
    protected Map<String, Set<Edge>> inEdges;
    private final TinkerGraph graph;
    private boolean allowNullPropertyValues;

    protected TinkerVertex(final Object id, final String label, final TinkerGraph graph) {
        super(id, label);
        this.graph = graph;
        this.allowNullPropertyValues = graph.features().vertex().supportsNullPropertyValues();
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.removed) return VertexProperty.empty();
        if (TinkerHelper.inComputerMode(this.graph)) {
            final List<VertexProperty> list = (List) this.graph.graphComputerView.getProperty(this, key);
            if (list.size() == 0)
                return VertexProperty.<V>empty();
            else if (list.size() == 1)
                return list.get(0);
            else
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
        } else {
            if (this.properties != null && this.properties.containsKey(key)) {
                final List<VertexProperty> list = (List) this.properties.get(key);
                if (list.size() > 1)
                    throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
                else
                    return list.get(0);
            } else
                return VertexProperty.<V>empty();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);

        // if we don't allow null property values and the value is null then the key can be removed but only if the
        // cardinality is single. if it is list/set then we can just ignore the null.
        if (!allowNullPropertyValues && null == value) {
            final VertexProperty.Cardinality card = null == cardinality ? graph.features().vertex().getCardinality(key) : cardinality;
            if (VertexProperty.Cardinality.single == card)
                properties(key).forEachRemaining(VertexProperty::remove);
            return VertexProperty.empty();
        }

        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

        if (TinkerHelper.inComputerMode(this.graph)) {
            final VertexProperty<V> vertexProperty = (VertexProperty<V>) this.graph.graphComputerView.addProperty(this, key, value);
            ElementHelper.attachProperties(vertexProperty, keyValues);
            return vertexProperty;
        } else {
            final Object idValue = optionalId.isPresent() ?
                    graph.vertexPropertyIdManager.convert(optionalId.get()) :
                    graph.vertexPropertyIdManager.getNextId(graph);

            final VertexProperty<V> vertexProperty = new TinkerVertexProperty<V>(idValue, this, key, value);

            if (null == this.properties) this.properties = new HashMap<>();
            final List<VertexProperty> list = this.properties.getOrDefault(key, new ArrayList<>());
            list.add(vertexProperty);
            this.properties.put(key, list);
            TinkerHelper.autoUpdateIndex(this, key, value, null);
            ElementHelper.attachProperties(vertexProperty, keyValues);
            return vertexProperty;
        }
    }

    @Override
    public Set<String> keys() {
        if (null == this.properties) return Collections.emptySet();
        return TinkerHelper.inComputerMode((TinkerGraph) graph()) ?
                Vertex.super.keys() :
                this.properties.keySet();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);
        return TinkerHelper.addEdge(this.graph, this, (TinkerVertex) vertex, label, keyValues);
    }

    @Override
    public void remove() {
        final List<Edge> edges = new ArrayList<>();
        this.edges(Direction.BOTH).forEachRemaining(edges::add);
        edges.stream().filter(edge -> !((TinkerEdge) edge).removed).forEach(Edge::remove);
        this.properties = null;
        TinkerHelper.removeElementIndex(this);
        this.graph.vertices.remove(this.id);
        this.removed = true;
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        final Iterator<Edge> edgeIterator = (Iterator) TinkerHelper.getEdges(this, direction, edgeLabels);
        return TinkerHelper.inComputerMode(this.graph) ?
                IteratorUtils.filter(edgeIterator, edge -> this.graph.graphComputerView.legalEdge(this, edge)) :
                edgeIterator;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return TinkerHelper.inComputerMode(this.graph) ?
                direction.equals(Direction.BOTH) ?
                        IteratorUtils.concat(
                                IteratorUtils.map(this.edges(Direction.OUT, edgeLabels), Edge::inVertex),
                                IteratorUtils.map(this.edges(Direction.IN, edgeLabels), Edge::outVertex)) :
                        IteratorUtils.map(this.edges(direction, edgeLabels), edge -> edge.vertices(direction.opposite()).next()) :
                (Iterator) TinkerHelper.getVertices(this, direction, edgeLabels);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if (this.removed) return Collections.emptyIterator();
        if (TinkerHelper.inComputerMode((TinkerGraph) graph()))
            return (Iterator) ((TinkerGraph) graph()).graphComputerView.getProperties(TinkerVertex.this).stream().filter(p -> ElementHelper.keyExists(p.key(), propertyKeys)).iterator();
        else {
            if (null == this.properties) return Collections.emptyIterator();
            if (propertyKeys.length == 1) {
                final List<VertexProperty> properties = this.properties.getOrDefault(propertyKeys[0], Collections.emptyList());
                if (properties.size() == 1) {
                    return IteratorUtils.of(properties.get(0));
                } else if (properties.isEmpty()) {
                    return Collections.emptyIterator();
                } else {
                    return (Iterator) new ArrayList<>(properties).iterator();
                }
            } else
                return (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).flatMap(entry -> entry.getValue().stream()).collect(Collectors.toList()).iterator();
        }
    }
}
