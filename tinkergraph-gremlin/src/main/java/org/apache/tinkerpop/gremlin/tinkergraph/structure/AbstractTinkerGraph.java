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

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputerView;
import org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerServiceRegistry;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Base class for {@link TinkerGraph} and {@link TinkerTransactionGraph}.
 * Contains common methods, variables and constants, but leaves the work with elements and indices
 * to concrete implementations.
 *
 * @author Valentyn Kahamlyk
 */
public abstract class AbstractTinkerGraph implements Graph {

    public static final String GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER = "gremlin.tinkergraph.vertexIdManager";
    public static final String GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER = "gremlin.tinkergraph.edgeIdManager";
    public static final String GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER = "gremlin.tinkergraph.vertexPropertyIdManager";
    public static final String GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY = "gremlin.tinkergraph.defaultVertexPropertyCardinality";
    public static final String GREMLIN_TINKERGRAPH_GRAPH_LOCATION = "gremlin.tinkergraph.graphLocation";
    public static final String GREMLIN_TINKERGRAPH_GRAPH_FORMAT = "gremlin.tinkergraph.graphFormat";
    public static final String GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES = "gremlin.tinkergraph.allowNullPropertyValues";
    public static final String GREMLIN_TINKERGRAPH_SERVICE = "gremlin.tinkergraph.service";


    protected AtomicLong currentId = new AtomicLong(-1L);

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
    protected String graphFormat;

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
     * {@inheritDoc}
     */
    public abstract boolean hasEdge(final Object id);

    protected void loadGraph() {
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) {
            try {
                if (graphFormat.equals("graphml")) {
                    io(IoCore.graphml()).readGraph(graphLocation);
                } else if (graphFormat.equals("graphson")) {
                    io(IoCore.graphson()).readGraph(graphLocation);
                } else if (graphFormat.equals("gryo")) {
                    io(IoCore.gryo()).readGraph(graphLocation);
                } else {
                    io(IoCore.createIoBuilder(graphFormat)).readGraph(graphLocation);
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Could not load graph at %s with %s", graphLocation, graphFormat), ex);
            }
        }
    }

    protected void saveGraph() {
        final File f = new File(graphLocation);
        if (f.exists()) {
            f.delete();
        } else {
            final File parent = f.getParentFile();

            // the parent would be null in the case of an relative path if the graphLocation was simply: "f.gryo"
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
        }

        try {
            if (graphFormat.equals("graphml")) {
                io(IoCore.graphml()).writeGraph(graphLocation);
            } else if (graphFormat.equals("graphson")) {
                io(IoCore.graphson()).writeGraph(graphLocation);
            } else if (graphFormat.equals("gryo")) {
                io(IoCore.gryo()).writeGraph(graphLocation);
            } else {
                io(IoCore.createIoBuilder(graphFormat)).writeGraph(graphLocation);
            }
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Could not save graph at %s with %s", graphLocation, graphFormat), ex);
        }
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
    public void clear() {
        this.variables = null;
        this.currentId.set(-1L);
        this.vertexIndex = null;
        this.edgeIndex = null;
        this.graphComputerView = null;
    }

    /**
     * This method only has an effect if the {@link TinkerGraph#GREMLIN_TINKERGRAPH_GRAPH_LOCATION} is set, in which case the
     * data in the graph is persisted to that location. This method may be called multiple times and does not release
     * resources.
     */
    @Override
    public void close() {
        if (graphLocation != null) saveGraph();
        // shutdown services
        serviceRegistry.close();
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

        private final TinkerTransactionGraph.TinkerGraphVertexPropertyFeatures vertexPropertyFeatures = new TinkerTransactionGraph.TinkerGraphVertexPropertyFeatures();

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
        T getNextId(final AbstractTinkerGraph graph);

        /**
         * Convert an identifier to the type required by the manager.
         */
        T convert(final Object id);

        /**
         * Determine if an identifier is allowed by this manager given its type.
         */
        boolean allow(final Object id);
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
            public Long getNextId(final AbstractTinkerGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.hasVertex(id) && !graph.hasEdge(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Long)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).longValue();
                else if (id instanceof String) {
                    try {
                        return Long.parseLong((String) id);
                    } catch (NumberFormatException nfe) {
                        throw new IllegalArgumentException(createErrorMessage(Long.class, id));
                    }
                }
                else
                    throw new IllegalArgumentException(createErrorMessage(Long.class, id));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof Number || id instanceof String;
            }
        },

        /**
         * Manages identifiers of type {@code Integer}. Will convert any class that extends from {@link Number} to a
         * {@link Integer} and will also attempt to convert {@code String} values
         */
        INTEGER {
            @Override
            public Integer getNextId(final AbstractTinkerGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).map(Long::intValue).filter(id -> !graph.hasVertex(id) && !graph.hasEdge(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Integer)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).intValue();
                else if (id instanceof String) {
                    try {
                        return Integer.parseInt((String) id);
                    } catch (NumberFormatException nfe) {
                        throw new IllegalArgumentException(createErrorMessage(Integer.class, id));
                    }
                }
                else
                    throw new IllegalArgumentException(createErrorMessage(Integer.class, id));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof Number || id instanceof String;
            }
        },

        /**
         * Manages identifiers of type {@code UUID}. Will convert {@code String} values to
         * {@code UUID}.
         */
        UUID {
            @Override
            public java.util.UUID getNextId(final AbstractTinkerGraph graph) {
                return java.util.UUID.randomUUID();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof java.util.UUID)
                    return id;
                else  if (id instanceof String) {
                    try {
                        return java.util.UUID.fromString((String) id);
                    } catch (IllegalArgumentException iae) {
                        throw new IllegalArgumentException(createErrorMessage(java.util.UUID.class, id));
                    }
                } else
                    throw new IllegalArgumentException(createErrorMessage(java.util.UUID.class, id));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof UUID || id instanceof String;
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
            public Long getNextId(final AbstractTinkerGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.hasVertex(id) && !graph.hasEdge(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                return id;
            }

            @Override
            public boolean allow(final Object id) {
                return true;
            }
        },
        
        /**
         * Manages identifiers of type {@code String}.
         */
        STRING {
            @Override
            public String getNextId(final AbstractTinkerGraph graph) {
                return java.util.UUID.randomUUID().toString();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else  if (id instanceof String) {
                    if (((String)id).isEmpty())
                        throw new IllegalArgumentException("Expected a non-empty string but received an empty string.");

                    return id;
                } else
                    throw new IllegalArgumentException(createErrorMessage(java.lang.String.class, id));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof String && !((String)id).isEmpty();
            }
        };

        private static String createErrorMessage(final Class<?> expectedType, final Object id) {
            return String.format("Expected an id that is convertible to %s but received %s - [%s]", expectedType, id.getClass(), id);
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
