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

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputerView;
import org.apache.tinkerpop.gremlin.gql.GqlDeclarativeMatchStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerServiceRegistry;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.storage.DefaultStorage;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.storage.TinkerStorage;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class for {@link TinkerMemoryGraph} and {@link TinkerStorageGraph}.
 * Contains common methods, variables and constants, but leaves the work with elements and indices
 * to concrete implementations.
 *
 * @author Valentyn Kahamlyk
 */
public abstract class AbstractTinkerGraph implements TinkerGraph {

    protected AtomicLong currentId = new AtomicLong(-1L);
    protected Map<Object, VertexProperty> vertexProperties = new ConcurrentHashMap<>();

    protected TinkerGraphVariables variables = null;
    protected TinkerGraphComputerView graphComputerView = null;
    protected AbstractTinkerIndex<TinkerVertex> vertexIndex = null;
    protected AbstractTinkerIndex<TinkerEdge> edgeIndex = null;

    protected IdManager<Vertex> vertexIdManager;
    protected IdManager<Edge> edgeIdManager;
    protected IdManager<VertexProperty> vertexPropertyIdManager;
    protected VertexProperty.Cardinality defaultVertexPropertyCardinality;
    protected boolean allowNullPropertyValues;

    protected TinkerServiceRegistry serviceRegistry;

    protected Configuration configuration;
    protected String graphLocation;

    /**
     * The pluggable durable storage engine, or {@code null} when the graph holds data only in memory. Only set by
     * transactional implementations that support persistence.
     */
    protected TinkerStorage storage;

    /**
     * Guard set while a graph is replaying its storage log on open. While {@code true}, mutations must not be
     * re-persisted, otherwise replay would append the loaded data back to the log.
     */
    protected volatile boolean loading = false;

    /**
     * {@inheritDoc}
     */
    public abstract Vertex addVertex(final Object... keyValues);

    /**
     * {@inheritDoc}
     */
    public abstract void removeVertex(final Object vertexId);

    /**
     * {@inheritDoc}
     */
    public abstract Edge addEdge(final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues);

    /**
     * {@inheritDoc}
     */
    public abstract void removeEdge(final Object edgeId);

    /**
     * Mark {@link Vertex} as changed in transaction.
     * If the graph does not support transactions, then does nothing.
     * @param vertex
     */
    public void touch(final TinkerVertex vertex) {};

    /**
     * Mark {@link Edge} as changed in transaction.
     * If the graph does not support transactions, then does nothing.
     * @param edge
     */
    public void touch(final TinkerEdge edge) {};

    /**
     * Determines if this graph stores elements in transactional containers, meaning elements are held by id
     * reference and cloned on read rather than referenced directly. Returns {@code false} unless overridden by an
     * implementation that supports transactions.
     */
    public boolean isTxMode() {
        return false;
    }

    /**
     * Return {@link Vertex} by id.
     * Does not create an iterator, so is the preferred method when only 1 element needs to be returned.
     * @param vertexId
     * @return Vertex
     */
    public abstract Vertex vertex(final Object vertexId);

    /**
     * {@inheritDoc}
     */
    public abstract Iterator<Vertex> vertices(final Object... vertexIds);

    /**
     * Return {@link Edge} by id.
     * Does not create an iterator, so is the preferred method when only 1 element needs to be returned.
     * @param edgeId
     * @return Edge
     */
    public abstract Edge edge(final Object edgeId);

    /**
     * {@inheritDoc}
     */
    public abstract Iterator<Edge> edges(final Object... edgeIds);

    /**
     * {@inheritDoc}
     */
    public abstract Transaction tx();

    /**
     * Graph-specific implementation for number of vertices.
     * @return count of vertices in Graph.
     */
    public abstract int getVerticesCount();

    /**
     * {@inheritDoc}
     */
    public abstract boolean hasVertex(final Object id);

    /**
     * Graph-specific implementation for number of vertices.
     * @return count of vertices in Graph.
     */
    public abstract int getEdgesCount();

    /**
     * Returns the number of vertices with the given label. A {@code null} label returns the
     * total vertex count. The default implementation does a full vertex scan; subclasses that
     * maintain a label index should override this for O(1) performance.
     */
    @Override
    public long countVerticesByLabel(final String label) {
        if (label == null) return getVerticesCount();
        long count = 0;
        final Iterator<Vertex> it = vertices();
        while (it.hasNext()) {
            if (label.equals(it.next().label())) count++;
        }
        return count;
    }

    /**
     * Returns the number of edges with the given label. A {@code null} label returns the
     * total edge count. The default implementation does a full edge scan; subclasses that
     * maintain a label index should override this for O(1) performance.
     */
    @Override
    public long countEdgesByLabel(final String label) {
        if (label == null) return getEdgesCount();
        long count = 0;
        final Iterator<Edge> it = edges();
        while (it.hasNext()) {
            if (label.equals(it.next().label())) count++;
        }
        return count;
    }

    /**
     * {@link Graph.Index} implementation backed by TinkerGraph's property index structures.
     * Returns {@code Long.MAX_VALUE} from {@link Graph.Index#countVertexIndex} when the key
     * is not indexed, signalling to the GQL executor that a full scan is required.
     */
    private final class TinkerGraphIndex implements Graph.Index {
        @Override
        public Iterator<Vertex> queryVertexIndex(final String key, final Object value) {
            return TinkerIndexHelper.queryVertexIndex(AbstractTinkerGraph.this, key, value)
                    .stream().map(v -> (Vertex) v).iterator();
        }

        @Override
        public long countVertexIndex(final String key, final Object value) {
            if (vertexIndex == null) return Long.MAX_VALUE;
            // TinkerIndex.count() returns 0 for keys that are not indexed, which is
            // indistinguishable from "indexed but empty". Use getIndexedKeys() to distinguish.
            if (!getIndexedKeys(Vertex.class).contains(key)) return Long.MAX_VALUE;
            return TinkerIndexHelper.countVertexIndex(AbstractTinkerGraph.this, key, value);
        }
    }

    private final Graph.Index tinkerGraphIndex = new TinkerGraphIndex();

    @Override
    public Graph.Index index() {
        return tinkerGraphIndex;
    }

    /**
     * {@inheritDoc}
     */
    public abstract boolean hasEdge(final Object id);

    /**
     * Returns true if a {@link VertexProperty} with the given identifier exists in this graph.
     */
    public boolean hasVertexProperty(final Object id) {
        return vertexProperties.containsKey(id);
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        if (builder.requiresVersion(GryoVersion.V1_0) || builder.requiresVersion(GraphSONVersion.V1_0))
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV1.instance())).create();
        else if (builder.requiresVersion(GraphSONVersion.V2_0))   // there is no gryo v2
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV2.instance())).create();
        else if (builder.requiresVersion(GraphSONVersion.V3_0))
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV3.instance())).create();
        else
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV4.instance())).create();
    }

    ////////////// STRUCTURE API METHODS //////////////////
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        if (!graphComputerClass.equals(TinkerGraphComputer.class))
            throw Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
        return (C) new TinkerGraphComputer(this);
    }

    public GraphComputer compute() { return new TinkerGraphComputer(this); }

    public Variables variables() {
        if (null == this.variables)
            this.variables = new TinkerGraphVariables();
        return this.variables;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.getVerticesCount() + " edges:" + this.getEdgesCount());
    }

    /**
     * Clear internal graph data
     */
    @Override
    public void clear() {
        this.variables = null;
        this.currentId.set(-1L);
        this.vertexIndex = null;
        this.edgeIndex = null;
        this.graphComputerView = null;
        this.vertexProperties.clear();
    }

    /**
     * Closes the graph, releasing any resources held by its {@link TinkerServiceRegistry}. This method may be called
     * multiple times and is a no-op with respect to graph data for the in-memory implementation. Transactional
     * implementations that are backed by a {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.storage.TinkerStorage}
     * engine flush and close that engine here.
     */
    @Override
    public void close() {
        if (storage != null) {
            storage.flush();
            storage.compact(this);
            storage.close();
        }
        serviceRegistry.close();
        GqlDeclarativeMatchStrategy.evict(this);
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    ///////////// Utility methods ///////////////
    protected abstract void addOutEdge(final TinkerVertex vertex, final String label, final Edge edge);

    protected abstract void addInEdge(final TinkerVertex vertex, final String label, final Edge edge);

    protected TinkerVertex createTinkerVertex(final Object id, final String label, final AbstractTinkerGraph graph) {
        return new TinkerVertex(id, label, graph);
    }

    protected TinkerVertex createTinkerVertex(final Object id, final String label, final AbstractTinkerGraph graph, final long currentVersion) {
        return new TinkerVertex(id, label, graph, currentVersion);
    }

    protected TinkerEdge createTinkerEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex) {
        return new TinkerEdge(id, outVertex, label, inVertex);
    }

    protected TinkerEdge createTinkerEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex, final long currentVersion) {
        return new TinkerEdge(id, outVertex, label, inVertex, currentVersion);
    }

    ///////////// Features ///////////////

    public class TinkerGraphVertexFeatures implements Features.VertexFeatures {

        private final TinkerGraphVertexPropertyFeatures vertexPropertyFeatures = new TinkerGraphVertexPropertyFeatures();

        protected TinkerGraphVertexFeatures() {
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return allowNullPropertyValues;
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return defaultVertexPropertyCardinality;
        }
    }

    public class TinkerGraphEdgeFeatures implements Features.EdgeFeatures {

        protected TinkerGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return allowNullPropertyValues;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return edgeIdManager.allow(id);
        }
    }

    public class TinkerGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

        protected TinkerGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return allowNullPropertyValues;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Return all the keys currently being index for said element class  ({@link Vertex} or {@link Edge}).
     *
     * @param elementClass the element class to get the indexed keys for
     * @param <E>          The type of the element class
     * @return the set of keys currently being indexed
     */
    @Override
    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return null == this.vertexIndex ? Collections.emptySet() : this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return null == this.edgeIndex ? Collections.emptySet() : this.edgeIndex.getIndexedKeys();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    ///////////// Id manager ///////////////
    /**
     * Construct an {@link IdManager} from the TinkerGraph {@code Configuration}.
     */
    protected static <T extends Element> IdManager<T> selectIdManager(final Configuration config, final String configKey, final Class<T> clazz) {
        final String idManagerConfigValue = config.getString(configKey, DefaultIdManager.ANY.name());
        try {
            return DefaultIdManager.valueOf(idManagerConfigValue);
        } catch (IllegalArgumentException iae) {
            try {
                return (IdManager) Class.forName(idManagerConfigValue).newInstance();
            } catch (Exception ex) {
                throw new IllegalStateException(String.format("Could not configure TinkerGraph %s id manager with %s", clazz.getSimpleName(), idManagerConfigValue));
            }
        }
    }

    ///////////// Storage engine ///////////////
    /**
     * Construct a {@link TinkerStorage} engine from the TinkerGraph {@code Configuration}, or return {@code null} when
     * no storage engine is configured. The configuration value is either a {@link DefaultStorage} enum name (matched
     * case-insensitively, e.g. {@code graphbinary}) or the fully-qualified class name of a {@link TinkerStorage}
     * implementation with a public no-argument constructor. Mirrors {@link #selectIdManager}.
     */
    protected static TinkerStorage selectStorage(final Configuration config, final String configKey) {
        final String storageConfigValue = config.getString(configKey, null);
        if (null == storageConfigValue)
            return null;
        try {
            return DefaultStorage.valueOf(storageConfigValue.toUpperCase()).get();
        } catch (IllegalArgumentException iae) {
            try {
                return (TinkerStorage) Class.forName(storageConfigValue).newInstance();
            } catch (Exception ex) {
                throw new IllegalStateException(String.format("Could not configure TinkerGraph storage engine with %s", storageConfigValue), ex);
            }
        }
    }

    protected TinkerServiceRegistry.TinkerServiceFactory instantiate(final String className) {
        try {
            return (TinkerServiceRegistry.TinkerServiceFactory) Class.forName(className).getConstructor(AbstractTinkerGraph.class).newInstance(this);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
                 NoSuchMethodException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }
}
