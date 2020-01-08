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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.util.Host;

import java.util.Iterator;

/**
 * A {@link Vertex} maintains pointers to both a set of incoming and outgoing {@link Edge} objects. The outgoing edges
 * are those edges for  which the {@link Vertex} is the tail. The incoming edges are those edges for which the
 * {@link Vertex} is the head.
 * <p/>
 * Diagrammatically:
 * <pre>
 * ---inEdges---&gt; vertex ---outEdges---&gt;.
 * </pre>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Vertex extends Element, Host {

    /**
     * The default label to use for a vertex.
     */
    public static final String DEFAULT_LABEL = "vertex";

    static final Object[] EMPTY_ARGS = new Object[0];

    /**
     * Add an outgoing edge to the vertex with provided label and edge properties as key/value pairs.
     * These key/values must be provided in an even number where the odd numbered arguments are {@link String}
     * property keys and the even numbered arguments are the related property values.
     *
     * @param label     The label of the edge
     * @param inVertex  The vertex to receive an incoming edge from the current vertex
     * @param keyValues The key/value pairs to turn into edge properties
     * @return the newly created edge
     */
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues);

    /**
     * Get the {@link VertexProperty} for the provided key. If the property does not exist, return
     * {@link VertexProperty#empty}. If there are more than one vertex properties for the provided
     * key, then throw {@link Vertex.Exceptions#multiplePropertiesExistForProvidedKey}.
     *
     * @param key the key of the vertex property to get
     * @param <V> the expected type of the vertex property value
     * @return the retrieved vertex property
     */
    @Override
    public default <V> VertexProperty<V> property(final String key) {
        final Iterator<VertexProperty<V>> iterator = this.properties(key);
        if (iterator.hasNext()) {
            final VertexProperty<V> property = iterator.next();
            if (iterator.hasNext())
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return property;
        } else {
            return VertexProperty.<V>empty();
        }
    }

    /**
     * Set the provided key to the provided value using {@link VertexProperty.Cardinality#single}.
     *
     * @param key   the key of the vertex property
     * @param value The value of the vertex property
     * @param <V>   the type of the value of the vertex property
     * @return the newly created vertex property
     */
    @Override
    public default <V> VertexProperty<V> property(final String key, final V value) {
        return this.property(key, value, EMPTY_ARGS);
    }

    /**
     * Set the provided key to the provided value using default {@link VertexProperty.Cardinality} for that key.
     * The default cardinality can be vendor defined and is usually tied to the graph schema.
     * The default implementation of this method determines the cardinality
     * {@code graph().features().vertex().getCardinality(key)}. The provided key/values are the properties of the
     * newly created {@link VertexProperty}. These key/values must be provided in an even number where the odd
     * numbered arguments are {@link String}.
     *
     * @param key       the key of the vertex property
     * @param value     The value of the vertex property
     * @param keyValues the key/value pairs to turn into vertex property properties
     * @param <V>       the type of the value of the vertex property
     * @return the newly created vertex property
     */
    public default <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        return this.property(graph().features().vertex().getCardinality(key), key, value, keyValues);
    }

    /**
     * Create a new vertex property. If the cardinality is {@link VertexProperty.Cardinality#single}, then set the key
     * to the value. If the cardinality is {@link VertexProperty.Cardinality#list}, then add a new value to the key.
     * If the cardinality is {@link VertexProperty.Cardinality#set}, then only add a new value if that value doesn't
     * already exist for the key. If the value already exists for the key, add the provided key value vertex property
     * properties to it.
     *
     * @param cardinality the desired cardinality of the property key
     * @param key         the key of the vertex property
     * @param value       The value of the vertex property
     * @param keyValues   the key/value pairs to turn into vertex property properties
     * @param <V>         the type of the value of the vertex property
     * @return the newly created vertex property
     */
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues);

    /**
     * Gets an {@link Iterator} of incident edges.
     *
     * @param direction  The incident direction of the edges to retrieve off this vertex
     * @param edgeLabels The labels of the edges to retrieve. If no labels are provided, then get all edges.
     * @return An iterator of edges meeting the provided specification
     */
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels);

    /**
     * Gets an {@link Iterator} of adjacent vertices.
     *
     * @param direction  The adjacency direction of the vertices to retrieve off this vertex
     * @param edgeLabels The labels of the edges associated with the vertices to retrieve. If no labels are provided,
     *                   then get all edges.
     * @return An iterator of vertices meeting the provided specification
     */
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels);

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys);

    /**
     * Common exceptions to use with a vertex.
     */
    public static class Exceptions {

        private Exceptions() {
        }

        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers");
        }

        public static UnsupportedOperationException userSuppliedIdsOfThisTypeNotSupported() {
            return new UnsupportedOperationException("Vertex does not support user supplied identifiers of this type");
        }

        public static IllegalStateException vertexRemovalNotSupported() {
            return new IllegalStateException("Vertex removal are not supported");
        }

        public static IllegalStateException edgeAdditionsNotSupported() {
            return new IllegalStateException("Edge additions not supported");
        }

        public static IllegalStateException multiplePropertiesExistForProvidedKey(final String propertyKey) {
            return new IllegalStateException("Multiple properties exist for the provided key, use Vertex.properties(" + propertyKey + ')');
        }
    }
}
