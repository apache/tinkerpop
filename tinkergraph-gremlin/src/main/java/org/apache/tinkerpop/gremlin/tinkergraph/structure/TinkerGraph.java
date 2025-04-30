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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory (with optional persistence on calls to {@link #close()}), reference implementation of the property
 * graph interfaces provided by TinkerPop.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_LIMITED_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_LIMITED_COMPUTER)
public class TinkerGraph extends AbstractTinkerGraph {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(TinkerGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                TinkerGraphStepStrategy.instance(),
                TinkerGraphCountStrategy.instance()));
    }

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
    }};

    private final TinkerGraphFeatures features = new TinkerGraphFeatures();

    protected Map<Object, Vertex> vertices = new ConcurrentHashMap<>();
    protected Map<Object, Edge> edges = new ConcurrentHashMap<>();

    /**
     * An empty private constructor that initializes {@link TinkerGraph}.
     */
    TinkerGraph(final Configuration configuration) {
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
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> If a {@link Graph} implementation does not require a {@code Configuration}
     * (or perhaps has a default configuration) it can choose to implement a zero argument
     * {@code open()} method. This is an optional constructor method for TinkerGraph. It is not enforced by the Gremlin
     * Test Suite.
     */
    public static TinkerGraph open() {
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
    public static TinkerGraph open(final Configuration configuration) {
        return new TinkerGraph(configuration);
    }

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = vertexIdManager.getNextId(this);
        }

        final Vertex vertex = createTinkerVertex(idValue, label, this);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        this.vertices.put(vertex.id(), vertex);

        return vertex;
    }

    @Override
    public void removeVertex(final Object vertexId)
    {
        this.vertices.remove(vertexId);
    }

    @Override
    public Edge addEdge(final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));

        final Edge edge;
        if (null != idValue) {
            if (edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = edgeIdManager.getNextId(this);
        }

        edge = new TinkerEdge(idValue, outVertex, label, inVertex);
        ElementHelper.attachProperties(edge, keyValues);
        edges.put(edge.id(), edge);
        addOutEdge(outVertex, label, edge);
        addInEdge(inVertex, label, edge);
        return edge;
    }

    @Override
    public void removeEdge(final Object edgeId) {
        final Edge edge = edges.get(edgeId);
        // already removed?
        if (null == edge) return;

        final TinkerVertex outVertex = (TinkerVertex) edge.outVertex();
        final TinkerVertex inVertex = (TinkerVertex) edge.inVertex();

        if (null != outVertex && null != outVertex.outEdges) {
            final Set<Edge> edges = outVertex.outEdges.get(edge.label());
            if (null != edges)
                edges.removeIf(e -> e.id() == edgeId);
        }
        if (null != inVertex && null != inVertex.inEdges) {
            final Set<Edge> edges = inVertex.inEdges.get(edge.label());
            if (null != edges)
                edges.removeIf(e -> e.id() == edgeId);
        }

        this.edges.remove(edgeId);
    }

    @Override
    public void clear() {
        super.clear();
        this.vertices.clear();
        this.edges.clear();
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public int getVerticesCount() { return vertices.size(); }

    @Override
    public boolean hasVertex(Object id) { return vertices.containsKey(id); }

    @Override
    public int getEdgesCount() {  return edges.size(); }

    @Override
    public boolean hasEdge(Object id) { return edges.containsKey(id); }

    @Override
    public TinkerServiceRegistry getServiceRegistry() {
        return serviceRegistry;
    }

    @Override
    public Vertex vertex(final Object vertexId) {
        return vertices.get(vertexIdManager.convert(vertexId));
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return createElementIterator(Vertex.class, vertices, vertexIdManager, vertexIds);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return createElementIterator(Edge.class, edges, edgeIdManager, edgeIds);
    }

    @Override
    public Edge edge(final Object edgeId) {
        return edges.get(edgeIdManager.convert(edgeId));
    }


    private <T extends Element> Iterator<T> createElementIterator(final Class<T> clazz, final Map<Object, T> elements,
                                                                  final IdManager idManager,
                                                                  final Object... ids) {
        final Iterator<T> iterator;
        if (0 == ids.length) {
            iterator = new TinkerGraphIterator<>(elements.values().iterator());
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
                return elements.get(idManager.convert(iid));
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
        if (null == vertex.outEdges) vertex.outEdges = new HashMap<>();
        Set<Edge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    @Override
    protected void addInEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        if (null == vertex.inEdges) vertex.inEdges = new HashMap<>();
        Set<Edge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.inEdges.put(label, edges);
        }
        edges.add(edge);
    }

    /**
     * Return TinkerGraph feature set.
     * <p/>
     * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
     * negative or instance configured features.  By default, all {@link Graph.Features} return true.
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
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public boolean supportsServiceCall() {
            return true;
        }

    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Create a default index for said element class ({@link Vertex} or {@link Edge}) and said property key.
     * Whenever an element has the specified key mutated, the index is updated.
     * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param key          the property key to index
     * @param elementClass the element class to index
     * @param <E>          The type of the element class
     */
    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        createIndex(TinkerIndexType.DEFAULT, key, elementClass, Collections.emptyMap());
    }

    /**
     * Create an index for said element class ({@link Vertex} or {@link Edge}) and said property key with the given
     * configuration options. Whenever an element has the specified key mutated, the index is updated. When the index
     * is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param indexType     the type of the index
     * @param key           the property key to index
     * @param elementClass  the element class to index
     * @param configuration the configuration options
     * @param <E>           The type of the element class
     */
    public <E extends Element> void createIndex(final TinkerIndexType indexType, final String key,
                                                final Class<E> elementClass, final Map<String, Object> configuration) {
        if (TinkerIndexType.VECTOR == indexType) {
            if (Vertex.class.isAssignableFrom(elementClass)) {
                if (null == this.vertexVectorIndex) this.vertexVectorIndex = new TinkerVectorIndex<>(this, TinkerVertex.class);
                this.vertexVectorIndex.createIndex(key, configuration);
            } else if (Edge.class.isAssignableFrom(elementClass)) {
                if (null == this.edgeVectorIndex) this.edgeVectorIndex = new TinkerVectorIndex<>(this, TinkerEdge.class);
                this.edgeVectorIndex.createIndex(key, configuration);
            } else {
                throw new IllegalArgumentException("Class is not indexable: " + elementClass);
            }
        } else {
            // Create a standard index
            if (Vertex.class.isAssignableFrom(elementClass)) {
                if (null == this.vertexIndex) this.vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
                this.vertexIndex.createIndex(key);
            } else if (Edge.class.isAssignableFrom(elementClass)) {
                if (null == this.edgeIndex) this.edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);
                this.edgeIndex.createIndex(key);
            } else {
                throw new IllegalArgumentException("Class is not indexable: " + elementClass);
            }
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
            if (null != this.vertexIndex) this.vertexIndex.dropIndex(key);
            if (null != this.vertexVectorIndex) this.vertexVectorIndex.dropIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null != this.edgeIndex) this.edgeIndex.dropIndex(key);
            if (null != this.edgeVectorIndex) this.edgeVectorIndex.dropIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Find the nearest vertices to the given vector in the vector index for the specified property key.
     *
     * @param key    the property key
     * @param vector the query vector
     * @param k      the number of nearest neighbors to return
     * @return a list of vertices sorted by distance
     */
    public List<Vertex> findNearestVertices(final String key, final float[] vector, final int k) {
        if (null == this.vertexVectorIndex)
            return Collections.emptyList();
        return new ArrayList<>(this.vertexVectorIndex.findNearest(key, vector, k));
    }

    /**
     * Find the nearest vertices to the given vector in the vector index for the specified property key.
     * Uses the default number of nearest neighbors.
     *
     * @param key    the property key
     * @param vector the query vector
     * @return a list of vertices sorted by distance
     */
    public List<Vertex> findNearestVertices(final String key, final float[] vector) {
        if (null == this.vertexVectorIndex)
            return Collections.emptyList();
        return new ArrayList<>(this.vertexVectorIndex.findNearest(key, vector));
    }

    /**
     * Find the nearest edges to the given vector in the vector index for the specified property key.
     *
     * @param key    the property key
     * @param vector the query vector
     * @param k      the number of nearest neighbors to return
     * @return a list of edges sorted by distance
     */
    public List<Edge> findNearestEdges(final String key, final float[] vector, final int k) {
        if (null == this.edgeVectorIndex)
            return Collections.emptyList();
        return new ArrayList<>(this.edgeVectorIndex.findNearest(key, vector, k));
    }

    /**
     * Find the nearest edges to the given vector in the vector index for the specified property key.
     * Uses the default number of nearest neighbors.
     *
     * @param key    the property key
     * @param vector the query vector
     * @return a list of edges sorted by distance
     */
    public List<Edge> findNearestEdges(final String key, final float[] vector) {
        if (null == this.edgeVectorIndex)
            return Collections.emptyList();
        return new ArrayList<>(this.edgeVectorIndex.findNearest(key, vector));
    }
}
