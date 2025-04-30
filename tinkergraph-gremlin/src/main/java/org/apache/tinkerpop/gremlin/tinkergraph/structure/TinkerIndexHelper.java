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

import java.util.Collections;
import java.util.List;

public final class TinkerIndexHelper {

    private TinkerIndexHelper() {}

    /**
     * Find nearest neighbors in the vertex vector index.
     *
     * @param graph  the graph
     * @param key    the property key
     * @param vector the query vector
     * @param k      the number of nearest neighbors to return
     * @return a list of vertices sorted by distance
     */
    public static List<TinkerVertex> findNearestVertices(final AbstractTinkerGraph graph, final String key, final float[] vector, final int k) {
        return null == graph.vertexVectorIndex ? Collections.emptyList() : graph.vertexVectorIndex.findNearest(key, vector, k);
    }

    /**
     * Find nearest neighbors in the vertex vector index with default k.
     *
     * @param graph  the graph
     * @param key    the property key
     * @param vector the query vector
     * @return a list of vertices sorted by distance
     */
    public static List<TinkerVertex> findNearestVertices(final AbstractTinkerGraph graph, final String key, final float[] vector) {
        return null == graph.vertexVectorIndex ? Collections.emptyList() : graph.vertexVectorIndex.findNearest(key, vector);
    }

    /**
     * Find nearest neighbors in the edge vector index.
     *
     * @param graph  the graph
     * @param key    the property key
     * @param vector the query vector
     * @param k      the number of nearest neighbors to return
     * @return a list of edges sorted by distance
     */
    public static List<TinkerEdge> findNearestEdges(final AbstractTinkerGraph graph, final String key, final float[] vector, final int k) {
        return null == graph.edgeVectorIndex ? Collections.emptyList() : graph.edgeVectorIndex.findNearest(key, vector, k);
    }

    /**
     * Find nearest neighbors in the edge vector index with default k.
     *
     * @param graph  the graph
     * @param key    the property key
     * @param vector the query vector
     * @return a list of edges sorted by distance
     */
    public static List<TinkerEdge> findNearestEdges(final AbstractTinkerGraph graph, final String key, final float[] vector) {
        return null == graph.edgeVectorIndex ? Collections.emptyList() : graph.edgeVectorIndex.findNearest(key, vector);
    }

    public static List<TinkerVertex> queryVertexIndex(final AbstractTinkerGraph graph, final String key, final Object value) {
        return null == graph.vertexIndex ? Collections.emptyList() : graph.vertexIndex.get(key, value);
    }

    public static List<TinkerEdge> queryEdgeIndex(final AbstractTinkerGraph graph, final String key, final Object value) {
        return null == graph.edgeIndex ? Collections.emptyList() : graph.edgeIndex.get(key, value);
    }

    public static void autoUpdateIndex(final TinkerEdge edge, final String key, final Object newValue, final Object oldValue) {
        final AbstractTinkerGraph graph = (AbstractTinkerGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.autoUpdate(key, newValue, oldValue, edge);
        if (graph.edgeVectorIndex != null && newValue instanceof float[])
            graph.edgeVectorIndex.autoUpdate(key, newValue, oldValue, edge);
    }

    public static void autoUpdateIndex(final TinkerVertex vertex, final String key, final Object newValue, final Object oldValue) {
        final AbstractTinkerGraph graph = (AbstractTinkerGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.autoUpdate(key, newValue, oldValue, vertex);
        if (graph.vertexVectorIndex != null && newValue instanceof float[])
            graph.vertexVectorIndex.autoUpdate(key, newValue, oldValue, vertex);
    }

    public static void removeElementIndex(final TinkerVertex vertex) {
        final AbstractTinkerGraph graph = (AbstractTinkerGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.removeElement(vertex);
        if (graph.vertexVectorIndex != null)
            graph.vertexVectorIndex.removeElement(vertex);
    }

    public static void removeElementIndex(final TinkerEdge edge) {
        final AbstractTinkerGraph graph = (AbstractTinkerGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.removeElement(edge);
        if (graph.edgeVectorIndex != null)
            graph.edgeVectorIndex.removeElement(edge);
    }

    public static void removeIndex(final TinkerVertex vertex, final String key, final Object value) {
        final AbstractTinkerGraph graph = (AbstractTinkerGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.remove(key, value, vertex);
        if (graph.vertexVectorIndex != null && value instanceof float[])
            graph.vertexVectorIndex.remove(key, value, vertex);
    }

    public static void removeIndex(final TinkerEdge edge, final String key, final Object value) {
        final AbstractTinkerGraph graph = (AbstractTinkerGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.remove(key, value, edge);
        if (graph.edgeVectorIndex != null && value instanceof float[])
            graph.edgeVectorIndex.remove(key, value, edge);
    }
}
