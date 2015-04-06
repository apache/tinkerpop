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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphStepStrategy;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * An in-sideEffects, reference implementation of the property graph interfaces provided by Gremlin3.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_PERFORMANCE)
public class TinkerGraph implements Graph {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(TinkerGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(TinkerGraphStepStrategy.instance()));
    }

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
    }};

    protected Long currentId = -1l;
    protected Map<Object, Vertex> vertices = new ConcurrentHashMap<>();
    protected Map<Object, Edge> edges = new ConcurrentHashMap<>();

    protected TinkerGraphVariables variables = null;
    protected TinkerGraphView graphView = null;
    protected TinkerIndex<TinkerVertex> vertexIndex = null;
    protected TinkerIndex<TinkerEdge> edgeIndex = null;

    private final static TinkerGraph EMPTY_GRAPH = new TinkerGraph();

    /**
     * An empty private constructor that initializes {@link TinkerGraph}.
     */
    private TinkerGraph() {
    }

    public static TinkerGraph empty() {
        return EMPTY_GRAPH;
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> If a {@link org.apache.tinkerpop.gremlin.structure.Graph } implementation does not require a
     * {@link org.apache.commons.configuration.Configuration} (or perhaps has a default configuration) it can choose to implement a zero argument
     * open() method. This is an optional constructor method for TinkerGraph. It is not enforced by the Gremlin
     * Test Suite.
     */
    public static TinkerGraph open() {
        return open(null);
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the
     * {@link org.apache.tinkerpop.gremlin.structure.util.GraphFactory} to instantiate
     * {@link org.apache.tinkerpop.gremlin.structure.Graph} instances.  This method must be overridden for the Structure Test
     * Suite to pass. Implementers have latitude in terms of how exceptions are handled within this method.  Such
     * exceptions will be considered implementation specific by the test suite as all test generate graph instances
     * by way of {@link org.apache.tinkerpop.gremlin.structure.util.GraphFactory}. As such, the exceptions get generalized
     * behind that facade and since {@link org.apache.tinkerpop.gremlin.structure.util.GraphFactory} is the preferred method
     * to opening graphs it will be consistent at that level.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link org.apache.tinkerpop.gremlin.structure.Graph}
     */
    public static TinkerGraph open(final Configuration configuration) {
        return new TinkerGraph();
    }

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = TinkerHelper.getNextId(this);
        }

        final Vertex vertex = new TinkerVertex(idValue, label, this);
        this.vertices.put(vertex.id(), vertex);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        return vertex;
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        if (!graphComputerClass.equals(TinkerGraphComputer.class))
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
        return (C) new TinkerGraphComputer(this);
    }

    @Override
    public GraphComputer compute() {
        return new TinkerGraphComputer(this);
    }

    @Override
    public Variables variables() {
        if (null == this.variables)
            this.variables = new TinkerGraphVariables();
        return this.variables;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.variables = null;
        this.currentId = 0l;
        this.vertexIndex = null;
        this.edgeIndex = null;
    }

    @Override
    public void close() {
        this.graphView = null;
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Configuration configuration() {
        return EMPTY_CONFIGURATION;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        if (0 == vertexIds.length) {
            return this.vertices.values().iterator();
        } else if (1 == vertexIds.length) {
            final Vertex vertex = this.vertices.get(vertexIds[0]);
            return null == vertex ? Collections.emptyIterator() : IteratorUtils.of(vertex);
        } else
            return Stream.of(vertexIds).map(this.vertices::get).filter(Objects::nonNull).iterator();
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        if (0 == edgeIds.length) {
            return this.edges.values().iterator();
        } else if (1 == edgeIds.length) {
            final Edge edge = this.edges.get(edgeIds[0]);
            return null == edge ? Collections.emptyIterator() : IteratorUtils.of(edge);
        } else
            return Stream.of(edgeIds).map(this.edges::get).filter(Objects::nonNull).iterator();
    }

    /**
     * Return TinkerGraph feature set.
     * <p/>
     * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
     * negative or instance configured features.  By default, all {@link Features} return true.
     */
    @Override
    public Features features() {
        return TinkerGraphFeatures.INSTANCE;
    }

    public static class TinkerGraphFeatures implements Features {

        static final TinkerGraphFeatures INSTANCE = new TinkerGraphFeatures();

        private TinkerGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return TinkerGraphGraphFeatures.INSTANCE;
        }

        @Override
        public EdgeFeatures edge() {
            return TinkerGraphEdgeFeatures.INSTANCE;
        }

        @Override
        public VertexFeatures vertex() {
            return TinkerGraphVertexFeatures.INSTANCE;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

    public static class TinkerGraphVertexFeatures implements Features.VertexFeatures {
        static final TinkerGraphVertexFeatures INSTANCE = new TinkerGraphVertexFeatures();

        private TinkerGraphVertexFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public static class TinkerGraphEdgeFeatures implements Features.EdgeFeatures {
        static final TinkerGraphEdgeFeatures INSTANCE = new TinkerGraphEdgeFeatures();

        private TinkerGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public static class TinkerGraphGraphFeatures implements Features.GraphFeatures {
        static final TinkerGraphGraphFeatures INSTANCE = new TinkerGraphGraphFeatures();

        private TinkerGraphGraphFeatures() {
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

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
            if (null == this.vertexIndex) this.vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null == this.edgeIndex) this.edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);
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

    /**
     * Return all the keys currently being index for said element class  ({@link Vertex} or {@link Edge}).
     *
     * @param elementClass the element class to get the indexed keys for
     * @param <E>          The type of the element class
     * @return the set of keys currently being indexed
     */
    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return null == this.vertexIndex ? Collections.emptySet() : this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return null == this.edgeIndex ? Collections.emptySet() : this.edgeIndex.getIndexedKeys();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }
}
