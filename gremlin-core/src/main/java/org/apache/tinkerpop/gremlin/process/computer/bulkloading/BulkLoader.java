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
package org.apache.tinkerpop.gremlin.process.computer.bulkloading;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public interface BulkLoader {

    /**
     * Gets or creates a clone of the given vertex in the given graph.
     *
     * @param vertex The vertex to be cloned.
     * @param graph  The graph that holds the cloned vertex after this method was called.
     * @param g      A standard traversal source for the given graph.
     * @return The cloned vertex.
     */
    public Vertex getOrCreateVertex(final Vertex vertex, final Graph graph, final GraphTraversalSource g);

    /**
     * Creates a clone of the given edge between the given in- and out-vertices.
     *
     * @param edge      The edge to be cloned.
     * @param outVertex The out-vertex in the given graph..
     * @param inVertex  The in-vertex in the given graph.
     * @param graph     The graph that holds the cloned edge after this method was called.
     * @param g         A standard traversal source for the given graph.
     * @return The cloned edge.
     */
    public default Edge createEdge(final Edge edge, final Vertex outVertex, final Vertex inVertex, final Graph graph, final GraphTraversalSource g) {
        final Edge result = outVertex.addEdge(edge.label(), inVertex);
        edge.properties().forEachRemaining(property -> result.property(property.key(), property.value()));
        return result;
    }

    /**
     * Gets or creates a clone of the given edge between the given in- and out-vertices.
     *
     * @param edge      The edge to be cloned.
     * @param outVertex The out-vertex in the given graph..
     * @param inVertex  The in-vertex in the given graph.
     * @param graph     The graph that holds the cloned edge after this method was called.
     * @param g         A standard traversal source for the given graph.
     * @return The cloned edge.
     */
    public Edge getOrCreateEdge(final Edge edge, final Vertex outVertex, final Vertex inVertex, final Graph graph, final GraphTraversalSource g);

    /**
     * Creates a clone of the given property for the given vertex.
     *
     * @param property The property to be cloned.
     * @param vertex   The vertex in the given graph..
     * @param graph    The graph that holds the given vertex.
     * @param g        A standard traversal source for the given graph.
     * @return The cloned property.
     */
    public default VertexProperty createVertexProperty(final VertexProperty<?> property, final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
        final VertexProperty result = vertex.property(property.key(), property.value());
        property.properties().forEachRemaining(metaProperty -> result.property(metaProperty.key(), metaProperty.value()));
        return result;
    }

    /**
     * Gets or creates a clone of the given property for the given vertex.
     *
     * @param property The property to be cloned.
     * @param vertex   The vertex in the given graph..
     * @param graph    The graph that holds the given vertex.
     * @param g        A standard traversal source for the given graph.
     * @return The cloned property.
     */
    public VertexProperty getOrCreateVertexProperty(final VertexProperty<?> property, final Vertex vertex, final Graph graph, final GraphTraversalSource g);

    /**
     * Get a vertex that matches the given vertex from the given graph.
     *
     * @param vertex The vertex to be matched.
     * @param graph  The graph that holds the given vertex.
     * @param g      A standard traversal source for the given graph.
     * @return The matched vertex.
     */
    public Vertex getVertex(final Vertex vertex, final Graph graph, final GraphTraversalSource g);

    /**
     * Gets a vertex by its ID from the given graph.
     *
     * @param id    The vertex ID.
     * @param graph The graph that holds the vertex with the given ID.
     * @param g     A standard traversal source for the given graph.
     * @return The vertex with the given ID.
     */
    public default Vertex getVertexById(final Object id, final Graph graph, final GraphTraversalSource g) {
        return g.V().hasId(id).next();
    }

    /**
     * @return Whether to use user supplied identifiers or not.
     */
    public boolean useUserSuppliedIds();

    /**
     * @return Whether to keep the original vertex identifiers in the target graph or not.
     */
    public boolean keepOriginalIds();

    /**
     * @return The name of the vertex property that is used to store the original vertex id.
     */
    public default String getVertexIdProperty() {
        return BulkLoaderVertexProgram.DEFAULT_BULK_LOADER_VERTEX_ID;
    }

    /**
     * Configures the BulkLoader instance.
     *
     * @param configuration The BulkLoader configuration.
     */
    public void configure(final Configuration configuration);
}
