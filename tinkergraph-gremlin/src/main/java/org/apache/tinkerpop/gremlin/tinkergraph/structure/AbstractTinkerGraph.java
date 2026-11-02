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
import org.apache.tinkerpop.gremlin.structure.LabelCardinality;
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
    protected LabelCardinality vertexLabelCardinality;
    protected LabelCardinality edgeLabelCardinality;
    protected String defaultVertexLabel;
    protected String defaultEdgeLabel;

    protected TinkerServiceRegistry serviceRegistry;

    protected Configuration configuration;

    /**
     * Set once {@link #close()} has run, after which the graph refuses further use.
     */
    private volatile boolean closed = false;

    /**
     * {@inheritDoc}
     */
    @Override
    public final Vertex addVertex(final Object... keyValues) {
        checkOpen();
        return doAddVertex(keyValues);
    }

    /**
     * Implementation of {@link #addVertex}, called once the graph is known to be open.
     */
    protected abstract Vertex doAddVertex(final Object... keyValues);

    /**
     * Remove the {@link Vertex} with the given id.
     */
    public final void removeVertex(final Object vertexId) {
        checkOpen();
        doRemoveVertex(vertexId);
    }

    /**
     * Implementation of {@link #removeVertex}, called once the graph is known to be open.
     */
    protected abstract void doRemoveVertex(final Object vertexId);

    /**
     * Add an {@link Edge} between two vertices of this graph.
     */
    public final Edge addEdge(final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
        checkOpen();
        return doAddEdge(outVertex, inVertex, label, keyValues);
    }

    /**
     * Implementation of {@link #addEdge}, called once the graph is known to be open.
     */
    protected abstract Edge doAddEdge(final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues);

    /**
     * Remove the {@link Edge} with the given id.
     */
    public final void removeEdge(final Object edgeId) {
        checkOpen();
        doRemoveEdge(edgeId);
    }

    /**
     * Implementation of {@link #removeEdge}, called once the graph is known to be open.
     */
    protected abstract void doRemoveEdge(final Object edgeId);

    /**
     * Mark {@link Vertex} as changed in transaction. Every mutation made through a TinkerGraph element calls this
     * first, so it also refuses a change made through an element held from before the graph was closed.
     */
    public final void touch(final TinkerVertex vertex) {
        checkOpen();
        doTouch(vertex);
    }

    /**
     * Mark {@link Edge} as changed in transaction. The edge counterpart of {@link #touch(TinkerVertex)}.
     */
    public final void touch(final TinkerEdge edge) {
        checkOpen();
        doTouch(edge);
    }

    /**
     * Implementation of {@link #touch(TinkerVertex)}. If the graph does not support transactions, then does nothing.
     */
    protected void doTouch(final TinkerVertex vertex) {}

    /**
     * Implementation of {@link #touch(TinkerEdge)}. If the graph does not support transactions, then does nothing.
     */
    protected void doTouch(final TinkerEdge edge) {}

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
     */
    public final Vertex vertex(final Object vertexId) {
        checkOpen();
        return doVertex(vertexId);
    }

    /**
     * Implementation of {@link #vertex}, called once the graph is known to be open.
     * Element reads and iterators already being consumed also call it after the graph is closed, so they can
     * keep reading the in-memory state as it stood at close.
     */
    protected abstract Vertex doVertex(final Object vertexId);

    /**
     * {@inheritDoc}
     */
    @Override
    public final Iterator<Vertex> vertices(final Object... vertexIds) {
        checkOpen();
        return doVertices(vertexIds);
    }

    /**
     * Implementation of {@link #vertices}, called once the graph is known to be open.
     * Element reads and iterators already being consumed also call it after the graph is closed, so they can
     * keep reading the in-memory state as it stood at close.
     */
    protected abstract Iterator<Vertex> doVertices(final Object... vertexIds);

    /**
     * Return {@link Edge} by id.
     * Does not create an iterator, so is the preferred method when only 1 element needs to be returned.
     */
    public final Edge edge(final Object edgeId) {
        checkOpen();
        return doEdge(edgeId);
    }

    /**
     * Implementation of {@link #edge}, called once the graph is known to be open.
     * Element reads and iterators already being consumed also call it after the graph is closed, so they can
     * keep reading the in-memory state as it stood at close.
     */
    protected abstract Edge doEdge(final Object edgeId);

    /**
     * {@inheritDoc}
     */
    @Override
    public final Iterator<Edge> edges(final Object... edgeIds) {
        checkOpen();
        return doEdges(edgeIds);
    }

    /**
     * Implementation of {@link #edges}, called once the graph is known to be open.
     * Element reads and iterators already being consumed also call it after the graph is closed, so they can
     * keep reading the in-memory state as it stood at close.
     */
    protected abstract Iterator<Edge> doEdges(final Object... edgeIds);

    /**
     * {@inheritDoc}
     */
    @Override
    public final Transaction tx() {
        checkOpen();
        return doTx();
    }

    /**
     * Implementation of {@link #tx}, called once the graph is known to be open.
     * Element reads and iterators already being consumed also call it after the graph is closed, so they can
     * keep reading the in-memory state as it stood at close.
     */
    protected abstract Transaction doTx();

    /**
     * The number of vertices in the graph.
     */
    public final int getVerticesCount() {
        checkOpen();
        return doGetVerticesCount();
    }

    /**
     * Implementation of {@link #getVerticesCount}, called once the graph is known to be open.
     */
    protected abstract int doGetVerticesCount();

    /**
     * Returns true if a {@link Vertex} with the given identifier exists in this graph.
     */
    public final boolean hasVertex(final Object id) {
        checkOpen();
        return doHasVertex(id);
    }

    /**
     * Implementation of {@link #hasVertex}, called once the graph is known to be open.
     */
    protected abstract boolean doHasVertex(final Object id);

    /**
     * The number of edges in the graph.
     */
    public final int getEdgesCount() {
        checkOpen();
        return doGetEdgesCount();
    }

    /**
     * Implementation of {@link #getEdgesCount}, called once the graph is known to be open.
     */
    protected abstract int doGetEdgesCount();

    /**
     * Returns the number of vertices with the given label. A {@code null} label returns the total vertex count.
     */
    @Override
    public final long countVerticesByLabel(final String label) {
        checkOpen();
        return doCountVerticesByLabel(label);
    }

    /**
     * Implementation of {@link #countVerticesByLabel}. The default implementation does a full vertex scan; subclasses
     * that maintain a label index should override this for O(1) performance.
     */
    protected long doCountVerticesByLabel(final String label) {
        if (label == null) return doGetVerticesCount();
        long count = 0;
        final Iterator<Vertex> it = doVertices();
        while (it.hasNext()) {
            if (it.next().labels().contains(label)) count++;
        }
        return count;
    }

    /**
     * Returns the number of edges with the given label. A {@code null} label returns the total edge count.
     */
    @Override
    public final long countEdgesByLabel(final String label) {
        checkOpen();
        return doCountEdgesByLabel(label);
    }

    /**
     * Implementation of {@link #countEdgesByLabel}. The default implementation does a full edge scan; subclasses
     * that maintain a label index should override this for O(1) performance.
     */
    protected long doCountEdgesByLabel(final String label) {
        if (label == null) return doGetEdgesCount();
        long count = 0;
        final Iterator<Edge> it = doEdges();
        while (it.hasNext()) {
            if (it.next().labels().contains(label)) count++;
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
            checkOpen();
            return TinkerIndexHelper.queryVertexIndex(AbstractTinkerGraph.this, key, value)
                    .stream().map(v -> (Vertex) v).iterator();
        }

        @Override
        public long countVertexIndex(final String key, final Object value) {
            checkOpen();
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
     * Returns true if an {@link Edge} with the given identifier exists in this graph.
     */
    public final boolean hasEdge(final Object id) {
        checkOpen();
        return doHasEdge(id);
    }

    /**
     * Implementation of {@link #hasEdge}, called once the graph is known to be open.
     */
    protected abstract boolean doHasEdge(final Object id);

    /**
     * Returns true if a {@link VertexProperty} with the given identifier exists in this graph.
     */
    public boolean hasVertexProperty(final Object id) {
        checkOpen();
        return vertexProperties.containsKey(id);
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        checkOpen();
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
        checkOpen();
        return (C) new TinkerGraphComputer(this);
    }

    public GraphComputer compute() {
        checkOpen();
        return new TinkerGraphComputer(this);
    }

    public Variables variables() {
        checkOpen();
        if (null == this.variables)
            this.variables = new TinkerGraphVariables();
        return this.variables;
    }

    @Override
    public String toString() {
        if (closed) return StringFactory.graphString(this, "closed");
        return StringFactory.graphString(this, "vertices:" + this.doGetVerticesCount() + " edges:" + this.doGetEdgesCount());
    }

    /**
     * Clear internal graph data
     */
    @Override
    public final void clear() {
        checkOpen();
        doClear();
    }

    /**
     * Implementation of {@link #clear()}. Subclasses that hold more data extend it and call this one.
     */
    protected void doClear() {
        this.variables = null;
        this.currentId.set(-1L);
        this.vertexIndex = null;
        this.edgeIndex = null;
        this.graphComputerView = null;
        this.vertexProperties.clear();
    }

    /**
     * Closes the graph, releasing any resources held by its {@link TinkerServiceRegistry}. Once closed, the graph
     * refuses further use: its methods, and mutations made through elements obtained from it, throw an
     * {@code IllegalStateException}. This method may be called multiple times.
     */
    @Override
    public void close() {
        try {
            serviceRegistry.close();
            GqlDeclarativeMatchStrategy.evict(this);
        } finally {
            closed = true;
        }
    }

    /**
     * Throws an {@code IllegalStateException} if the graph was closed. Every entry point of the graph, and the
     * {@link #touch} hook each element mutation runs through, calls this first.
     */
    protected void checkOpen() {
        if (closed)
            throw new IllegalStateException(String.format(
                    "%s is closed and can no longer be used; open a new graph instead", getClass().getSimpleName()));
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    ///////////// Utility methods ///////////////
    protected abstract void addOutEdge(final TinkerVertex vertex, final String label, final Edge edge);

    protected abstract void addInEdge(final TinkerVertex vertex, final String label, final Edge edge);

    /**
     * Add an edge to the per-vertex adjacency maps under the given label.
     * Used when labels are added to an existing edge.
     *
     * @param edge  the edge to register
     * @param label the label under which to register the edge
     * @since 4.0.0
     */
    public abstract void addEdgeToAdjacency(final TinkerEdge edge, final String label);

    /**
     * Remove an edge from the per-vertex adjacency maps for the given label.
     * Used when labels are removed from an existing edge.
     *
     * @param edge  the edge to unregister
     * @param label the label from which to unregister the edge
     * @since 4.0.0
     */
    public abstract void removeEdgeFromAdjacency(final TinkerEdge edge, final String label);

    /**
     * Called when one or more labels have been added to an existing vertex, allowing the graph to update
     * any internal label indices.
     *
     * @param vertex     the vertex to which labels were added
     * @param labelsAdded the labels that were newly added to the vertex
     * @since 4.0.0
     */
    public void addVertexLabels(final TinkerVertex vertex, final Set<String> labelsAdded) {
        // no-op by default - TinkerGraph does not maintain a separate label index
    }

    /**
     * Called when one or more labels have been removed from an existing vertex, allowing the graph to update
     * any internal label indices.
     *
     * @param vertex       the vertex from which labels were removed
     * @param labelsRemoved the labels that were actually removed from the vertex
     * @since 4.0.0
     */
    public void removeVertexLabels(final TinkerVertex vertex, final Set<String> labelsRemoved) {
        // no-op by default - TinkerGraph does not maintain a separate label index
    }

    protected TinkerVertex createTinkerVertex(final Object id, final String label, final AbstractTinkerGraph graph) {
        return new TinkerVertex(id, label, graph);
    }

    protected TinkerVertex createTinkerVertex(final Object id, final String label, final AbstractTinkerGraph graph, final long currentVersion) {
        return new TinkerVertex(id, label, graph, currentVersion);
    }

    /**
     * Creates a TinkerVertex with multiple labels.
     *
     * @param id     the vertex id
     * @param labels the set of labels for the vertex
     * @param graph  the graph instance
     * @return a new TinkerVertex
     * @since 4.0.0
     */
    protected TinkerVertex createTinkerVertex(final Object id, final Set<String> labels, final AbstractTinkerGraph graph) {
        return new TinkerVertex(id, labels, graph);
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

        @Override
        public LabelCardinality getLabelCardinality() {
            return vertexLabelCardinality;
        }

        @Override
        public String getDefaultLabel() {
            return defaultVertexLabel;
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
     * {@inheritDoc}
     */
    @Override
    public final <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        checkOpen();
        doCreateIndex(key, elementClass);
    }

    /**
     * Implementation of {@link #createIndex}, called once the graph is known to be open.
     */
    protected abstract <E extends Element> void doCreateIndex(final String key, final Class<E> elementClass);

    /**
     * {@inheritDoc}
     */
    @Override
    public final <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
        checkOpen();
        doDropIndex(key, elementClass);
    }

    /**
     * Implementation of {@link #dropIndex}, called once the graph is known to be open.
     */
    protected abstract <E extends Element> void doDropIndex(final String key, final Class<E> elementClass);

    /**
     * Return all the keys currently being index for said element class  ({@link Vertex} or {@link Edge}).
     *
     * @param elementClass the element class to get the indexed keys for
     * @param <E>          The type of the element class
     * @return the set of keys currently being indexed
     */
    @Override
    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        checkOpen();
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

    protected TinkerServiceRegistry.TinkerServiceFactory instantiate(final String className) {
        try {
            return (TinkerServiceRegistry.TinkerServiceFactory) Class.forName(className).getConstructor(AbstractTinkerGraph.class).newInstance(this);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
                 NoSuchMethodException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }
}
