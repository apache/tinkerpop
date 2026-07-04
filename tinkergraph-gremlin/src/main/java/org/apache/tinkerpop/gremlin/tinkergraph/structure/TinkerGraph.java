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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerServiceRegistry;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * TinkerPop's reference property graph, provided in two implementations: {@link TinkerMemoryGraph}, the in-memory
 * graph without transaction support, and {@link TinkerStorageGraph}, the transactional graph. The static
 * {@link #open()} methods on this interface construct the in-memory implementation, which preserves the historical
 * behavior of {@code TinkerGraph.open()}. {@link GraphFactory} may be given the name of this interface or of either
 * implementation for the {@code gremlin.graph} configuration key.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface TinkerGraph extends Graph {

    String GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER = "gremlin.tinkergraph.vertexIdManager";
    String GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER = "gremlin.tinkergraph.edgeIdManager";
    String GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER = "gremlin.tinkergraph.vertexPropertyIdManager";
    String GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY = "gremlin.tinkergraph.defaultVertexPropertyCardinality";
    String GREMLIN_TINKERGRAPH_GRAPH_LOCATION = "gremlin.tinkergraph.graphLocation";
    String GREMLIN_TINKERGRAPH_GRAPH_FORMAT = "gremlin.tinkergraph.graphFormat";
    String GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES = "gremlin.tinkergraph.allowNullPropertyValues";
    String GREMLIN_TINKERGRAPH_SERVICE = "gremlin.tinkergraph.service";

    /**
     * Open a new {@link TinkerMemoryGraph} instance.
     */
    static TinkerGraph open() {
        return TinkerMemoryGraph.open();
    }

    /**
     * Open a new {@link TinkerMemoryGraph} instance with the specified configuration. This method is the one used
     * by {@link GraphFactory} to instantiate {@link Graph} instances when the {@code gremlin.graph} configuration
     * key names this interface.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link Graph}
     */
    static TinkerGraph open(final Configuration configuration) {
        return TinkerMemoryGraph.open(configuration);
    }

    /**
     * Create an index for said element class ({@link Vertex} or {@link Edge}) and said property key.
     * Whenever an element has the specified key mutated, the index is updated.
     * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param key          the property key to index
     * @param elementClass the element class to index
     * @param <E>          The type of the element class
     */
    <E extends Element> void createIndex(final String key, final Class<E> elementClass);

    /**
     * Drop the index for the specified element class ({@link Vertex} or {@link Edge}) and key.
     *
     * @param key          the property key to stop indexing
     * @param elementClass the element class of the index to drop
     * @param <E>          The type of the element class
     */
    <E extends Element> void dropIndex(final String key, final Class<E> elementClass);

    /**
     * Return all the keys currently being index for said element class  ({@link Vertex} or {@link Edge}).
     *
     * @param elementClass the element class to get the indexed keys for
     * @param <E>          The type of the element class
     * @return the set of keys currently being indexed
     */
    <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass);

    /**
     * Clear internal graph data.
     */
    void clear();

    /**
     * TinkerGraph implementations do not throw checked exceptions on {@code close()}.
     */
    @Override
    void close();

    /**
     * Gets the registry of service factories for {@code call()} steps.
     */
    TinkerServiceRegistry getServiceRegistry();

    /**
     * TinkerGraph will use an implementation of this interface to generate identifiers when a user does not supply
     * them and to handle identifier conversions when querying to provide better flexibility with respect to
     * handling different data types that mean the same thing.  For example, the
     * {@link DefaultIdManager#LONG} implementation will allow {@code g.vertices(1l, 2l)} and
     * {@code g.vertices(1, 2)} to both return values.
     *
     * @param <T> the id type
     */
    interface IdManager<T> {
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
    enum DefaultIdManager implements IdManager {
        /**
         * Manages identifiers of type {@code Long}. Will convert any class that extends from {@link Number} to a
         * {@link Long} and will also attempt to convert {@code String} values
         */
        LONG {
            @Override
            public Long getNextId(final AbstractTinkerGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.hasVertex(id) && !graph.hasEdge(id) && !graph.hasVertexProperty(id)).findAny().get();
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
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).map(Long::intValue).filter(id -> !graph.hasVertex(id) && !graph.hasEdge(id) && !graph.hasVertexProperty(id)).findAny().get();
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
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.hasVertex(id) && !graph.hasEdge(id) && !graph.hasVertexProperty(id)).findAny().get();
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
}
