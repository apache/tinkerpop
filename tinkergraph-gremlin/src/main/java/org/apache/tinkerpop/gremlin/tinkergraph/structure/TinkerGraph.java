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
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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

    public static final String CONFIG_VERTEX_ID = "gremlin.tinkergraph.vertexIdManager";
    public static final String CONFIG_EDGE_ID = "gremlin.tinkergraph.edgeIdManager";
    public static final String CONFIG_VERTEX_PROPERTY_ID = "gremlin.tinkergraph.vertexPropertyIdManager";

    protected AtomicLong currentId = new AtomicLong(-1l);
    protected Map<Object, Vertex> vertices = new ConcurrentHashMap<>();
    protected Map<Object, Edge> edges = new ConcurrentHashMap<>();

    protected TinkerGraphVariables variables = null;
    protected TinkerGraphView graphView = null;
    protected TinkerIndex<TinkerVertex> vertexIndex = null;
    protected TinkerIndex<TinkerEdge> edgeIndex = null;

    private final static TinkerGraph EMPTY_GRAPH = new TinkerGraph(EMPTY_CONFIGURATION);

    protected final IdManager<?> vertexIdManager;
    protected final IdManager<?> edgeIdManager;
    protected final IdManager<?> vertexPropertyIdManager;

    private final Configuration configuration;

    /**
     * An empty private constructor that initializes {@link TinkerGraph}.
     */
    private TinkerGraph(final Configuration configuration) {
        this.configuration = configuration;
        vertexIdManager = selectIdManager(configuration, CONFIG_VERTEX_ID, Vertex.class);
        edgeIdManager = selectIdManager(configuration, CONFIG_EDGE_ID, Edge.class);
        vertexPropertyIdManager = selectIdManager(configuration, CONFIG_VERTEX_PROPERTY_ID, VertexProperty.class);
    }

    public static TinkerGraph empty() {
        return EMPTY_GRAPH;
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <br/>
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
     * <br/>
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
        this.currentId.set(-1l);
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
        return configuration;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return createElementIterator(Vertex.class, vertices, vertexIdManager, vertexIds);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return createElementIterator(Edge.class, edges, edgeIdManager, edgeIds);
    }

    private <T extends Element> Iterator<T> createElementIterator(final Class<T> clazz, final Map<Object, T> elements,
                                                                  final IdManager idManager,
                                                                  final Object... ids) {
        if (0 == ids.length) {
            return elements.values().iterator();
        } else if (1 == ids.length) {
            if (clazz.isAssignableFrom(ids[0].getClass())) {
                // no need to get the edge again, so just flip it back - some implementation may want to treat this
                // as a refresh operation. that's not necessary for tinkergraph.
                return IteratorUtils.of((T) ids[0]);
            } else {
                // convert the id to the expected data type and lookup the vertex
                final T element = elements.get(idManager.convert(ids[0]));
                return null == element ? Collections.emptyIterator() : IteratorUtils.of(element);
            }
        } else {
            // base the conversion function on the first item in the id list as the expectation is that these
            // id values will be a uniform list
            if (clazz.isAssignableFrom(ids[0].getClass())) {
                // based on the first item assume all vertices in the argument list
                if (!Stream.of(ids).allMatch(id -> clazz.isAssignableFrom(id.getClass())))
                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();

                // no need to get the vertices again, so just flip it back - some implementation may want to treat this
                // as a refresh operation. that's not necessary for tinkergraph.
                return Stream.of(ids).map(id -> (T) id).iterator();
            } else {
                final Class<?> firstClass = ids[0].getClass();
                if (!Stream.of(ids).map(Object::getClass).allMatch(firstClass::equals))
                    throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
                return Stream.of(ids).map(id -> idManager.convert(id)).map(elements::get).filter(Objects::nonNull).iterator();
            }
        }
    }

    /**
     * Return TinkerGraph feature set.
     * <br/>
     * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
     * negative or instance configured features.  By default, all
     * {@link org.apache.tinkerpop.gremlin.structure.Graph.Features} return true.
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

    /**
     * Construct an {@link TinkerGraph.IdManager} from the TinkerGraph {@code Configuration}.
     */
    private static IdManager<?> selectIdManager(final Configuration config, final String configKey, final Class<? extends Element> clazz) {
        final String vertexIdManagerConfigValue = config.getString(configKey, DefaultIdManager.ANY.name());
        try {
            return DefaultIdManager.valueOf(vertexIdManagerConfigValue);
        } catch (IllegalArgumentException iae) {
            try {
                return (IdManager) Class.forName(vertexIdManagerConfigValue).newInstance();
            } catch (Exception ex) {
                throw new IllegalStateException(String.format("Could not configure TinkerGraph %s id manager with %s", clazz.getSimpleName(), vertexIdManagerConfigValue));
            }
        }
    }

    /**
     * TinkerGraph will use an implementation of this interface to generate identifiers when a user does not supply
     * them and to handle identifier conversions when querying to provide better flexibility with respect to
     * handling different data types that mean the same thing.  For example, the
     * {@link DefaultIdManager#LONG} implementation will allow {@code g.vertices(1l, 2l)} and
     * {@code g.vertices(1, 2)} to both return values.
     *
     * @param <T> the id type
     */
    public interface IdManager<T> {
        /**
         * Generate an identifier which should be unique to the {@link TinkerGraph} instance.
         */
        T getNextId(final TinkerGraph graph);

        /**
         * Convert an identifier to the type required by the manager.
         */
        T convert(final Object id);
    }

    /**
     * A default set of {@link IdManager} implementations for common identifier types.
     */
    public enum DefaultIdManager implements IdManager {
        /**
         * Manages identifiers of type {@code Long}. Will convert any class that extends from {@link Number} to a
         * {@link Long} and will also attempt to convert {@code String} values
         */
        LONG {
            @Override
            public Long getNextId(final TinkerGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Long)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).longValue();
                else if (id instanceof String)
                    return Long.parseLong((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to Long but received %s", id.getClass()));
            }
        },

        /**
         * Manages identifiers of type {@code Integer}. Will convert any class that extends from {@link Number} to a
         * {@link Integer} and will also attempt to convert {@code String} values
         */
        INTEGER {
            @Override
            public Integer getNextId(final TinkerGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).map(Long::intValue).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Integer)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).intValue();
                else if (id instanceof String)
                    return Integer.parseInt((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to Integer but received %s", id.getClass()));
            }
        },

        /**
         * Manages identifiers of type {@link java.util.UUID}. Will convert {@code String} values to
         * {@link java.util.UUID}.
         */
        UUID {
            @Override
            public UUID getNextId(final TinkerGraph graph) {
                return java.util.UUID.randomUUID();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof java.util.UUID)
                    return id;
                else if (id instanceof String)
                    return java.util.UUID.fromString((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to UUID but received %s", id.getClass()));
            }
        },

        /**
         * Manages identifiers of any type.  This represents the default way {@link TinkerGraph} has always worked.
         * In other words, there is no identifier conversion so if the identifier of a vertex is a {@code Long}, then
         * trying to request it with an {@code Integer} will have no effect. Also, like the original
         * {@link TinkerGraph}, it will generate {@link Long} values for identifiers.
         */
        ANY {
            @Override
            public Long getNextId(final TinkerGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                return id;
            }
        }
    }
}
