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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphCountStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphStepStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerServiceRegistry;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory (with optional persistence on calls to {@link #close()}), reference implementation of the property
 * graph interfaces with transaction support provided by TinkerPop.
 *
 * @author Valentyn Kahamlyk
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_LIMITED_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_LIMITED_COMPUTER)
public final class TinkerTransactionGraph extends AbstractTinkerGraph {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(TinkerTransactionGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                TinkerGraphStepStrategy.instance(),
                TinkerGraphCountStrategy.instance()));
    }

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, TinkerTransactionGraph.class.getName());
    }};

    private final TinkerGraphFeatures features = new TinkerGraphFeatures();

    private final TinkerTransaction transaction = new TinkerTransaction(this);

    private final Map<Object, TinkerElementContainer<TinkerVertex>> vertices = new ConcurrentHashMap<>();
    private final Map<Object, TinkerElementContainer<TinkerEdge>> edges = new ConcurrentHashMap<>();

    /**
     * An empty private constructor that initializes {@link TinkerTransactionGraph}.
     */
    private TinkerTransactionGraph(final Configuration configuration) {
        this.configuration = configuration;
        vertexIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, Vertex.class);
        edgeIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, Edge.class);
        vertexPropertyIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, VertexProperty.class);
        defaultVertexPropertyCardinality = VertexProperty.Cardinality.valueOf(
                configuration.getString(GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.single.name()));
        allowNullPropertyValues = configuration.getBoolean(GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, false);

        graphLocation = configuration.getString(GREMLIN_TINKERGRAPH_GRAPH_LOCATION, null);
        graphFormat = configuration.getString(GREMLIN_TINKERGRAPH_GRAPH_FORMAT, null);

        if ((graphLocation != null && null == graphFormat) || (null == graphLocation && graphFormat != null))
            throw new IllegalStateException(String.format("The %s and %s must both be specified if either is present",
                    GREMLIN_TINKERGRAPH_GRAPH_LOCATION, GREMLIN_TINKERGRAPH_GRAPH_FORMAT));

        if (graphLocation != null) loadGraph();

        serviceRegistry = new TinkerServiceRegistry(this);
        configuration.getList(String.class, GREMLIN_TINKERGRAPH_SERVICE, Collections.emptyList()).forEach(serviceClass ->
                serviceRegistry.registerService(instantiate(serviceClass)));
    }

    /**
     * Open a new {@link TinkerTransactionGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> If a {@link Graph} implementation does not require a {@code Configuration}
     * (or perhaps has a default configuration) it can choose to implement a zero argument
     * {@code open()} method. This is an optional constructor method for TinkerGraph. It is not enforced by the Gremlin
     * Test Suite.
     */
    public static TinkerTransactionGraph open() {
        return open(EMPTY_CONFIGURATION);
    }

    /**
     * Open a new {@code TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the {@link GraphFactory} to instantiate
     * {@link Graph} instances.  This method must be overridden for the Structure Test Suite to pass. Implementers have
     * latitude in terms of how exceptions are handled within this method.  Such exceptions will be considered
     * implementation specific by the test suite as all test generate graph instances by way of
     * {@link GraphFactory}. As such, the exceptions get generalized behind that facade and since
     * {@link GraphFactory} is the preferred method to opening graphs it will be consistent at that level.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link Graph}
     */
    public static TinkerTransactionGraph open(final Configuration configuration) {
        return new TinkerTransactionGraph(configuration);
    }

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        if (null == idValue)
            idValue = vertexIdManager.getNextId(this);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        this.tx().readWrite();
        final long txNumber = transaction.getTxNumber();

        final TinkerElementContainer<TinkerVertex> newContainer = new TinkerElementContainer<>(idValue);
        // try to add new container or get existing
        TinkerElementContainer<TinkerVertex> container = vertices.putIfAbsent(idValue, newContainer);

        // is existing container contains Vertex?
        if (container != null && container.get() != null)
            throw Exceptions.vertexWithIdAlreadyExists(idValue);

        long version = txNumber;
        if (container != null && container.isDeleted() && container.getModified() != null) {
            // vertex being added was previously deleted 
            // we need to reference the version from the deleted state when adding the vertex back
            version = container.getModified().version();
            container.unmarkDeleted((TinkerTransaction) tx());
        }
        
        // no existing container, let's use new one
        if (container == null)
            container = newContainer;

        final TinkerVertex vertex = new TinkerVertex(idValue, label, this, version);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        container.setDraft(vertex, (TinkerTransaction) tx());

        return vertex;
    }

    @Override
    public void removeVertex(final Object vertexId) {
        if (!vertices.containsKey(vertexId)) return;

        // vertex can be deleted in other thread, so need to double-check
        final TinkerElementContainer<?> container = vertices.get(vertexId);
        if (null != container)
            container.markDeleted((TinkerTransaction) tx());
    }

    @Override
    public void touch(final TinkerVertex vertex) {
        // already removed, so skip
        if (null == vertex || !vertices.containsKey(vertex.id())) return;

        final TinkerElementContainer<TinkerVertex> container = vertices.get(vertex.id());

        if (null != container) {
            this.tx().readWrite();
            container.touch(vertex, (TinkerTransaction) tx());
        }
    }

    @Override
    public void touch(final TinkerEdge edge) {
        // already removed, so skip
        if (null == edge || !edges.containsKey(edge.id())) return;

        final TinkerElementContainer<TinkerEdge> container = edges.get(edge.id());

        if (null != container) {
            this.tx().readWrite();
            container.touch(edge, (TinkerTransaction) tx());
        }
    }

    @Override
    public Edge addEdge(final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        if (null == idValue)
            idValue = edgeIdManager.getNextId(this);

        this.tx().readWrite();
        final long txNumber = transaction.getTxNumber();

        final TinkerElementContainer<TinkerEdge> newContainer = new TinkerElementContainer<>(idValue);
        // try to add new container or get existing
        TinkerElementContainer<TinkerEdge> container = edges.putIfAbsent(idValue, newContainer);

        // is existing container contains Vertex?
        if (container != null && container.get() != null)
            throw Exceptions.vertexWithIdAlreadyExists(idValue);

        // no existing container, let's use new one
        if (container == null)
            container = newContainer;

        final TinkerEdge edge = new TinkerEdge(idValue, outVertex, label, inVertex, txNumber);
        ElementHelper.attachProperties(edge, keyValues);
        container.setDraft(edge, (TinkerTransaction) tx());

        addOutEdge(outVertex, label, edge);
        addInEdge(inVertex, label, edge);

        return edge;
    }

    @Override
    public void removeEdge(final Object edgeId) {
        if (!edges.containsKey(edgeId)) return;

        final TinkerElementContainer<TinkerEdge> container = edges.get(edgeId);

        if (null == container || container.isDeleted()) return;

        final TinkerEdge edge = container.get();

        if (edge == null) return;

        final TinkerVertex outVertex = (TinkerVertex) edge.outVertex();
        touch(outVertex);
        final TinkerVertex inVertex = (TinkerVertex) edge.inVertex();
        touch(inVertex);

        if (null != outVertex && null != outVertex.outEdgesId) {
            final Set<Object> edges = outVertex.outEdgesId.get(edge.label());
            if (null != edges) {
                edges.removeIf(e -> e == edge.id());
            }
        }
        if (null != inVertex && null != inVertex.inEdgesId) {
            final Set<Object> edges = inVertex.inEdgesId.get(edge.label());
            if (null != edges) {
                edges.removeIf(e -> e == edge.id());
            }
        }

        container.markDeleted((TinkerTransaction) tx());
    }

    @Override
    public void clear() {
        super.clear();
        this.vertices.clear();
        this.edges.clear();
    }

    @Override
    public Transaction tx() {
        return transaction;
    }

    @Override
    public int getVerticesCount() {
        return (int) vertices.entrySet().stream().filter(v -> v.getValue().get() != null).count();
    }

    @Override
    public boolean hasVertex(Object id) {
        return null != vertex(id);
    }

    Map<Object, TinkerElementContainer<TinkerVertex>> getVertices () { return vertices; }

    @Override
    public int getEdgesCount() {
        return (int) edges.entrySet().stream().filter(v -> v.getValue().get() != null).count();
    }

    @Override
    public boolean hasEdge(Object id) {
        return null != edge(id);
    }

    Map<Object, TinkerElementContainer<TinkerEdge>> getEdges () { return edges; }

    @Override
    public TinkerServiceRegistry getServiceRegistry() {
        return serviceRegistry;
    }

    @Override
    public Vertex vertex(final Object vertexId) {
        final TinkerElementContainer<TinkerVertex> container = vertices.get(vertexIdManager.convert(vertexId));
        return container == null ? null : container.getWithClone((TinkerTransaction) tx());
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return createElementIterator(Vertex.class, vertices, vertexIdManager, vertexIds);
    }

    @Override
    public Edge edge(final Object edgeId) {
        final TinkerElementContainer<TinkerEdge> container = edges.get(edgeIdManager.convert(edgeId));
        return container == null ? null : container.getWithClone((TinkerTransaction) tx());
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return createElementIterator(Edge.class, edges, edgeIdManager, edgeIds);
    }

    private <T extends Element, C extends TinkerElement> Iterator<T> createElementIterator(final Class<T> clazz,
                                                                  final Map<Object, TinkerElementContainer<C>> elements,
                                                                  final IdManager idManager,

                                                                  final Object... ids) {
        this.tx().readWrite();

        final Iterator<T> iterator;
        if (0 == ids.length) {
            iterator = new TinkerGraphIterator<>(
                    // todo: clone only if traversal contains mutating steps
                    elements.values().stream().map(c -> (T) c.getWithClone((TinkerTransaction) tx())).filter(e -> e != null).iterator());
        } else {
            final List<Object> idList = Arrays.asList(ids);

            // TinkerGraph can take a Vertex/Edge or any object as an "id". If it is an Element then we just cast
            // to that type and pop off the identifier. there is no need to pass that through the IdManager since
            // the assumption is that if it's already an Element, its identifier must be valid to the Graph and to
            // its associated IdManager. All other objects are passed to the IdManager for conversion.
            return new TinkerGraphIterator<>(IteratorUtils.filter(IteratorUtils.map(idList, id -> {
                // ids cant be null so all of those filter out
                if (null == id) return null;
                final Object iid = clazz.isAssignableFrom(id.getClass()) ? clazz.cast(id).id() : idManager.convert(id);
                final TinkerElementContainer<C> container = elements.get(iid);
                return container == null ? null : (T) container.getWithClone((TinkerTransaction) tx());
            }).iterator(), Objects::nonNull));
        }
        return TinkerHelper.inComputerMode(this) ?
                (Iterator<T>) (clazz.equals(Vertex.class) ?
                        IteratorUtils.filter((Iterator<Vertex>) iterator, t -> this.graphComputerView.legalVertex(t)) :
                        IteratorUtils.filter((Iterator<Edge>) iterator, t -> this.graphComputerView.legalEdge(t.outVertex(), t))) :
                iterator;
    }
    @Override
    protected void addOutEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        touch(vertex);
        if (null == vertex.outEdgesId) vertex.outEdgesId = new ConcurrentHashMap<>();
        Set<Object> edges = vertex.outEdgesId.get(label);
        if (null == edges) {
            edges = ConcurrentHashMap.newKeySet();
            vertex.outEdgesId.put(label, edges);
        }
        edges.add(edge.id());
    }

    @Override
    protected void addInEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        touch(vertex);
        if (null == vertex.inEdgesId) vertex.inEdgesId = new ConcurrentHashMap<>();
        Set<Object> edges = vertex.inEdgesId.get(label);
        if (null == edges) {
            edges = ConcurrentHashMap.newKeySet();
            vertex.inEdgesId.put(label, edges);
        }
        edges.add(edge.id());
    }

    /**
     * Return TinkerGraph feature set.
     * <p/>
     * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
     * negative or instance configured features.  By default, all {@link Features} return true.
     */
    @Override
    public Features features() {
        return features;
    }

    public class TinkerGraphFeatures implements Features {

        private final TinkerGraphGraphFeatures graphFeatures = new TinkerGraphGraphFeatures();
        private final TinkerGraphEdgeFeatures edgeFeatures = new TinkerGraphEdgeFeatures();
        private final TinkerGraphVertexFeatures vertexFeatures = new TinkerGraphVertexFeatures();

        private TinkerGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

    }

    public class TinkerGraphGraphFeatures implements Features.GraphFeatures {

        private TinkerGraphGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return true;
        }

        @Override
        public boolean supportsServiceCall() {
            return true;
        }

    }


    /////////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Create an index for said element class ({@link Vertex} or {@link Edge}) and said property key.
     * Whenever an element has the specified key mutated, the index is updated.
     * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param key          the property key to index
     * @param elementClass the element class to index
     * @param <E>          The type of the element class
     */
    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null == this.vertexIndex) this.vertexIndex = new TinkerTransactionalIndex<>(this, TinkerVertex.class);
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null == this.edgeIndex) this.edgeIndex = new TinkerTransactionalIndex<>(this, TinkerEdge.class);
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Drop the index for the specified element class ({@link Vertex} or {@link Edge}) and key.
     *
     * @param key          the property key to stop indexing
     * @param elementClass the element class of the index to drop
     * @param <E>          The type of the element class
     */
    public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null != this.vertexIndex) this.vertexIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null != this.edgeIndex) this.edgeIndex.dropKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }
}
