/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.structure.util.LabelCardinalityValidator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertex extends TinkerElement implements Vertex {

    protected Map<String, List<VertexProperty>> properties;
    // Edges should be used by non-transaction Graph due to performance
    protected Map<String, Set<Edge>> outEdges;
    protected Map<String, Set<Edge>> inEdges;
    // Edge ids are for transactional Graph
    protected Map<String, Set<Object>> outEdgesId;
    protected Map<String, Set<Object>> inEdgesId;
    protected final AbstractTinkerGraph graph;
    private boolean allowNullPropertyValues;
    private final boolean isTxMode;

    /**
     * Multi-label storage for this vertex. Starts as an immutable set (lightweight) and is
     * upgraded to a mutable ConcurrentHashMap-backed set on first label mutation.
     */
    protected Set<String> vertexLabels;

    protected TinkerVertex(final Object id, final String label, final AbstractTinkerGraph graph) {
        this(id, Collections.singleton(label != null ? label : Vertex.DEFAULT_LABEL), graph, -1);
    }

    protected TinkerVertex(final Object id, final String label, final AbstractTinkerGraph graph, final long currentVersion) {
        this(id, Collections.singleton(label != null ? label : Vertex.DEFAULT_LABEL), graph, currentVersion);
    }

    /**
     * Constructs a TinkerVertex with multiple labels.
     */
    protected TinkerVertex(final Object id, final Set<String> labels, final AbstractTinkerGraph graph) {
        this(id, labels, graph, -1);
    }

    /**
     * Canonical constructor. Constructs a TinkerVertex with multiple labels and a specific version (for transactional graphs).
     * Uses a single-switch pattern to handle default label injection per the configured cardinality.
     */
    protected TinkerVertex(final Object id, final Set<String> labels, final AbstractTinkerGraph graph, final long currentVersion) {
        super(id, null, currentVersion);  // labels are set below
        this.graph = graph;
        this.isTxMode = graph instanceof TinkerTransactionGraph;
        this.allowNullPropertyValues = graph.features().vertex().supportsNullPropertyValues();

        final Set<String> initial = new LinkedHashSet<>();
        switch (graph.vertexLabelCardinality) {
            case ONE:
                // Exactly 1 label: use provided or default
                if (labels != null && !labels.isEmpty())
                    initial.addAll(labels); // Will be rejected by LabelCardinalityValidator below if labels.size() > 1
                else
                    initial.add(graph.defaultVertexLabel);
                break;
            case ONE_OR_MORE:
                // Default label always present alongside any user labels
                initial.add(graph.defaultVertexLabel);
                if (labels != null) initial.addAll(labels);
                break;
            case ZERO_OR_MORE:
                if (labels != null) initial.addAll(labels);
                break;
        }

        // Validate the resulting set conforms to cardinality
        LabelCardinalityValidator.validateCreation(graph.vertexLabelCardinality, initial);

        // Store as immutable — will be upgraded on first mutation via ensureMutableLabels()
        this.vertexLabels = initial.size() <= 1
                ? (initial.isEmpty() ? Collections.emptySet() : Collections.singleton(initial.iterator().next()))
                : Collections.unmodifiableSet(initial);
    }

    /**
     * Upgrades the label set to a mutable ConcurrentHashMap-backed set on first mutation.
     * This is a one-time cost per vertex that actually needs label mutation.
     */
    private void ensureMutableLabels() {
        if (!(this.vertexLabels instanceof ConcurrentHashMap.KeySetView)) {
            final Set<String> mutable = ConcurrentHashMap.newKeySet();
            mutable.addAll(this.vertexLabels);
            this.vertexLabels = mutable;
        }
    }

    @Override
    public Set<String> labels() {
        if (this.vertexLabels.isEmpty()) {
            return Collections.emptySet();
        }
        // If already immutable (pre-mutation), return directly. If mutable (post-mutation), wrap.
        if (this.vertexLabels instanceof ConcurrentHashMap.KeySetView) {
            return Collections.unmodifiableSet(this.vertexLabels);
        }
        return this.vertexLabels; // already immutable from construction
    }

    @Override
    @Deprecated
    public String label() {
        if (this.vertexLabels.isEmpty()) {
            return "";
        }
        return this.vertexLabels.iterator().next();
    }

    @Override
    public void addLabel(final String label, final String... labels) {
        ElementHelper.validateLabel(label);
        for (final String l : labels) {
            ElementHelper.validateLabel(l);
        }
        LabelCardinalityValidator.validateAdd(this.graph.vertexLabelCardinality, this.vertexLabels, label, labels);
        graph.touch(this);
        ensureMutableLabels();
        this.vertexLabels.add(label);
        Collections.addAll(this.vertexLabels, labels);
        this.graph.updateVertexLabelIndex(this);
    }

    @Override
    public void dropLabels() {
        LabelCardinalityValidator.validateDropAll(this.graph.vertexLabelCardinality, this.vertexLabels);
        graph.touch(this);
        ensureMutableLabels();
        this.vertexLabels.clear();
        this.graph.updateVertexLabelIndex(this);
    }

    @Override
    public void dropLabel(final String label, final String... labels) {
        LabelCardinalityValidator.validateDrop(this.graph.vertexLabelCardinality, this.vertexLabels, label, labels);
        graph.touch(this);
        ensureMutableLabels();
        this.vertexLabels.remove(label);
        for (final String l : labels) {
            this.vertexLabels.remove(l);
        }
        this.graph.updateVertexLabelIndex(this);
    }

    @Override
    public Object clone() {
        if (!isTxMode) {
            final TinkerVertex vertex = new TinkerVertex(id, new LinkedHashSet<>(vertexLabels), graph);
            vertex.inEdgesId = inEdgesId;
            vertex.outEdgesId = outEdgesId;
            vertex.properties = properties;
            return vertex;
        }

        final TinkerVertex vertex = new TinkerVertex(id, new LinkedHashSet<>(vertexLabels), graph, currentVersion);
        if (inEdgesId != null)
            vertex.inEdgesId = CollectionUtil.clone((ConcurrentHashMap<String, Set<Object>>) inEdgesId);

        if (outEdgesId != null)
            vertex.outEdgesId = CollectionUtil.clone((ConcurrentHashMap<String, Set<Object>>) outEdgesId);

        if (properties != null) {
            final ConcurrentHashMap<String, List<VertexProperty>> result = new ConcurrentHashMap<>(properties.size());

            // clone will not work because TinkerVertexProperty contains link to Vertex
            for (Map.Entry<String, List<VertexProperty>> entry : properties.entrySet()) {
                final List<VertexProperty> clonedValue = entry.getValue().stream()
                        .map(vp ->((TinkerVertexProperty) vp).copy(vertex))
                        .collect(Collectors.toList());

                result.put(entry.getKey(), clonedValue);
            }
            vertex.properties = result;
        }

        return vertex;
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
                final List<VertexProperty> list = this.properties.get(key);
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
        graph.touch(this);

        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);

        // if we don't allow null property values and the value is null then the key can be removed but only if the
        // cardinality is single. if it is list/set then we can just ignore the null.
        if (!allowNullPropertyValues && null == value) {
            final VertexProperty.Cardinality card = null == cardinality ? graph.features().vertex().getCardinality(key) : cardinality;
            if (VertexProperty.Cardinality.single == card)
                properties(key).forEachRemaining(VertexProperty::remove);
            return VertexProperty.empty();
        }

        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);
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

            final VertexProperty<V> vertexProperty = createTinkerVertexProperty(idValue, this, key, value);

            if (null == this.properties) this.properties = new ConcurrentHashMap<>();
            final List<VertexProperty> list = this.properties.getOrDefault(key, new ArrayList<>());
            list.add(vertexProperty);
            this.properties.put(key, list);
            graph.vertexProperties.put(vertexProperty.id(), vertexProperty);
            TinkerIndexHelper.autoUpdateIndex(this, key, value, null);
            ElementHelper.attachProperties(vertexProperty, keyValues);
            return vertexProperty;
        }
    }

    @Override
    public Set<String> keys() {
        if (null == this.properties) return Collections.emptySet();
        return TinkerHelper.inComputerMode((AbstractTinkerGraph) graph()) ?
                Vertex.super.keys() :
                this.properties.keySet();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed || ((TinkerVertex) vertex).removed) throw elementAlreadyRemoved(Vertex.class, this.id);

        return graph.addEdge(this, (TinkerVertex) vertex, label, keyValues);
    }

    @Override
    public void remove() {
        graph.touch(this);

        final List<Edge> edges = new ArrayList<>();
        this.edges(Direction.BOTH).forEachRemaining(edge -> edges.add(edge));
        edges.stream().filter(edge -> !((TinkerEdge) edge).removed).forEach(Edge::remove);

        TinkerIndexHelper.removeElementIndex(this);
        if (null != this.properties)
            this.properties.values().forEach(vpList -> vpList.forEach(vp -> graph.vertexProperties.remove(vp.id())));
        this.properties = null;
        this.graph.removeVertex(this.id);
        this.removed = true;
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        final Iterator<Edge> edgeIterator = isTxMode
                ? (Iterator) TinkerHelper.getEdgesTx(this, direction, edgeLabels)
                : (Iterator) TinkerHelper.getEdges(this, direction, edgeLabels);
        return TinkerHelper.inComputerMode(this.graph) ?
                IteratorUtils.filter(edgeIterator, edge -> this.graph.graphComputerView.legalEdge(this, edge)) :
                edgeIterator;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        if (TinkerHelper.inComputerMode(this.graph))
            return direction.equals(Direction.BOTH) ?
                    IteratorUtils.concat(
                            IteratorUtils.map(this.edges(Direction.OUT, edgeLabels), Edge::inVertex),
                            IteratorUtils.map(this.edges(Direction.IN, edgeLabels), Edge::outVertex)) :
                    IteratorUtils.map(this.edges(direction, edgeLabels), edge -> edge.vertices(direction.opposite()).next());

        return isTxMode
                ? (Iterator) TinkerHelper.getVerticesTx(this, direction, edgeLabels)
                : (Iterator) TinkerHelper.getVertices(this, direction, edgeLabels);
    }

    protected <V> TinkerVertexProperty<V> createTinkerVertexProperty(final TinkerVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        return new TinkerVertexProperty<V>(vertex, key, value, propertyKeyValues);
    }

    protected <V> TinkerVertexProperty<V> createTinkerVertexProperty(final Object id, final TinkerVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        return new TinkerVertexProperty<V>(id, vertex, key, value, propertyKeyValues);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if (this.removed) return Collections.emptyIterator();
        if (TinkerHelper.inComputerMode((AbstractTinkerGraph) graph()))
            return (Iterator) ((AbstractTinkerGraph) graph()).graphComputerView.getProperties(TinkerVertex.this).stream().filter(p -> ElementHelper.keyExists(p.key(), propertyKeys)).iterator();
        else {
            if (null == this.properties) return Collections.emptyIterator();
            if (propertyKeys.length == 1) {
                if (null == propertyKeys[0])
                    return Collections.emptyIterator();
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
