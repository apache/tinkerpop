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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.EdgeScanOrder;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.EdgeSink;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.PropertySource;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.PropertyVisitor;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SnapshotSource;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SourceDefaults;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SourceVersion;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.UnsupportedSnapshotDataException;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.VertexPropertySource;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.VertexPropertyVisitor;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.VertexSink;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

/**
 * A {@link SnapshotSource} over a non-transactional {@link TinkerGraph}. The caller must guarantee exclusive access to
 * the graph while a snapshot is built from it.
 * <p/>
 * Every vertex label, every vertex property (all values of a key, in list order, each with its identifier and its
 * meta-properties), null values and the graph variables are emitted. Data the snapshot cannot represent is rejected with
 * {@link UnsupportedSnapshotDataException} before the element reaches the sink: edges without exactly one label and
 * null vertex property identifiers.
 * <p/>
 * Edges are emitted by walking the vertices in the same order as {@link #scanVertices} and emitting the out-edges of
 * each, which relies on the graph's iteration order repeating when the graph is not mutated.
 */
public final class TinkerGraphSnapshotSource implements SnapshotSource {

    private final AbstractTinkerGraph graph;
    private final String sourceId = "tinkergraph:" + UUID.randomUUID();

    /**
     * Wraps the graph. Transactional graphs are rejected.
     *
     * @throws IllegalArgumentException if the graph is transactional or not a TinkerGraph implementation that stores
     *                                  its elements directly
     */
    public TinkerGraphSnapshotSource(final TinkerGraph graph) {
        if (!(graph instanceof AbstractTinkerGraph))
            throw new IllegalArgumentException("Unsupported TinkerGraph implementation: " + graph.getClass().getName());
        final AbstractTinkerGraph abstractGraph = (AbstractTinkerGraph) graph;
        if (abstractGraph.isTxMode())
            throw new IllegalArgumentException("Snapshots of transactional TinkerGraph instances are not supported: " +
                    graph.getClass().getName());
        this.graph = abstractGraph;
    }

    @Override
    public SourceVersion version() {
        return new SourceVersion(sourceId, "vertices=" + graph.getVerticesCount() + ";edges=" + graph.getEdgesCount());
    }

    @Override
    public OptionalLong vertexCount() {
        return OptionalLong.of(graph.getVerticesCount());
    }

    @Override
    public OptionalLong edgeCount() {
        return OptionalLong.of(graph.getEdgesCount());
    }

    @Override
    public EdgeScanOrder edgeScanOrder() {
        return EdgeScanOrder.GROUPED_BY_OUT_VERTEX;
    }

    @Override
    public SourceDefaults defaults() {
        return new SourceDefaults(graph.defaultVertexLabel, graph.defaultEdgeLabel,
                graph.vertexLabelCardinality.name(), graph.defaultVertexPropertyCardinality.name());
    }

    @Override
    public void scanVertices(final VertexSink sink) {
        final MutableVertexProperties vertexProperties = new MutableVertexProperties();
        final List<String> labels = new ArrayList<>();
        final Iterator<Vertex> vertices = graph.vertices();
        while (vertices.hasNext()) {
            final TinkerVertex vertex = (TinkerVertex) vertices.next();
            labels.clear();
            for (final String label : vertex.vertexLabels) {
                if (null == label)
                    throw new UnsupportedSnapshotDataException(String.format("vertex [%s] has a null label", vertex.id()));
                labels.add(label);
            }
            validateVertexProperties(vertex);
            vertexProperties.vertex = vertex;
            sink.vertex(vertex.id(), labels, vertexProperties);
        }
        vertexProperties.vertex = null;
    }

    @Override
    public void scanVariables(final PropertyVisitor visitor) {
        final Graph.Variables variables = graph.variables();
        for (final String key : variables.keys()) {
            visitor.property(key, variables.get(key).orElse(null));
        }
    }

    @Override
    public void scanEdges(final EdgeSink sink) {
        final MutableEdgeProperties edgeProperties = new MutableEdgeProperties();
        final Iterator<Vertex> vertices = graph.vertices();
        while (vertices.hasNext()) {
            final TinkerVertex vertex = (TinkerVertex) vertices.next();
            final Map<String, Set<Edge>> outEdges = vertex.outEdges;
            if (null == outEdges) continue;

            for (final Set<Edge> edges : outEdges.values()) {
                for (final Edge e : edges) {
                    final TinkerEdge edge = (TinkerEdge) e;
                    final String label = singleLabel("edge", edge.id(), edge.edgeLabels);
                    edgeProperties.edge = edge;
                    sink.edge(edge.id(), label, edge.outVertex.id(), edge.inVertex.id(), edgeProperties);
                }
            }
        }
        edgeProperties.edge = null;
    }

    private static String singleLabel(final String kind, final Object id, final Set<String> labels) {
        if (labels.size() != 1)
            throw new UnsupportedSnapshotDataException(String.format(
                    "Edges require exactly one label but %s [%s] has %s: %s", kind, id, labels.size(), labels));
        final String label = labels.iterator().next();
        if (null == label)
            throw new UnsupportedSnapshotDataException(String.format("%s [%s] has a null label", kind, id));
        return label;
    }

    /**
     * Checks the vertex property identifiers up front so that unsupported data fails the scan even if the sink never
     * visits the properties.
     */
    private static void validateVertexProperties(final TinkerVertex vertex) {
        final Map<String, List<VertexProperty>> properties = vertex.properties;
        if (null == properties) return;

        for (final Map.Entry<String, List<VertexProperty>> entry : properties.entrySet()) {
            for (final VertexProperty vp : entry.getValue()) {
                if (null == vp.id())
                    throw new UnsupportedSnapshotDataException(String.format(
                            "Vertex property '%s' of vertex [%s] has a null identifier", entry.getKey(), vertex.id()));
            }
        }
    }

    /**
     * Reused for every vertex, as the contract allows because it is valid only during the sink call. Its content has
     * already been validated.
     */
    private static final class MutableVertexProperties implements VertexPropertySource {

        private TinkerVertex vertex;
        private final MutableMetaProperties metaProperties = new MutableMetaProperties();

        @Override
        public void forEach(final VertexPropertyVisitor visitor) {
            final Map<String, List<VertexProperty>> properties = vertex.properties;
            if (null == properties) return;

            for (final Map.Entry<String, List<VertexProperty>> entry : properties.entrySet()) {
                final String key = entry.getKey();
                for (final VertexProperty vp : entry.getValue()) {
                    metaProperties.vertexProperty = (TinkerVertexProperty<?>) vp;
                    visitor.vertexProperty(vp.id(), key, vp.value(), metaProperties);
                }
            }
            metaProperties.vertexProperty = null;
        }
    }

    /**
     * Reused for every vertex property, as the contract allows because it is valid only during the visitor call.
     */
    private static final class MutableMetaProperties implements PropertySource {

        private TinkerVertexProperty<?> vertexProperty;

        @Override
        public void forEach(final PropertyVisitor visitor) {
            final Map<String, Property> properties = vertexProperty.properties;
            if (null == properties) return;

            for (final Map.Entry<String, Property> entry : properties.entrySet()) {
                visitor.property(entry.getKey(), entry.getValue().value());
            }
        }
    }

    /**
     * Reused for every edge, as the contract allows because it is valid only during the sink call.
     */
    private static final class MutableEdgeProperties implements PropertySource {

        private TinkerEdge edge;

        @Override
        public void forEach(final PropertyVisitor visitor) {
            final Map<String, Property> properties = edge.properties;
            if (null == properties) return;

            for (final Map.Entry<String, Property> entry : properties.entrySet()) {
                visitor.property(entry.getKey(), entry.getValue().value());
            }
        }
    }
}
