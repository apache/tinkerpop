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

import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputerView;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerHelper {

    private TinkerHelper() {
    }

    public static Map<String, List<VertexProperty>> getProperties(final TinkerVertex vertex) {
        return null == vertex.properties ? Collections.emptyMap() : vertex.properties;
    }

    public static boolean inComputerMode(final AbstractTinkerGraph graph) {
        return null != graph.graphComputerView;
    }

    public static TinkerGraphComputerView createGraphComputerView(final AbstractTinkerGraph graph, final GraphFilter graphFilter, final Set<VertexComputeKey> computeKeys) {
        return graph.graphComputerView = new TinkerGraphComputerView(graph, graphFilter, computeKeys);
    }

    public static TinkerGraphComputerView getGraphComputerView(final AbstractTinkerGraph graph) {
        return graph.graphComputerView;
    }

    public static void dropGraphComputerView(final AbstractTinkerGraph graph) {
        graph.graphComputerView = null;
    }

    public static Iterator<TinkerEdge> getEdges(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Edge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdges.values().forEach(edges::addAll);
                else if (edgeLabels.length == 1)
                    edges.addAll(vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdges.values().forEach(edges::addAll);
                else if (edgeLabels.length == 1)
                    edges.addAll(vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
            }
        }
        return (Iterator) edges.iterator();
    }

    public static Iterator<TinkerEdge> getEdgesTx(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Object> outEdgeIds = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdgesId != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdgesId.values().forEach(outEdgeIds::addAll);
                else if (edgeLabels.length == 1)
                    outEdgeIds.addAll(vertex.outEdgesId.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(vertex.outEdgesId::get).filter(Objects::nonNull).forEach(outEdgeIds::addAll);
            }
        }

        final List<Object> inEdgeIds = new ArrayList<>();
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdgesId != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdgesId.values().forEach(inEdgeIds::addAll);
                else if (edgeLabels.length == 1)
                    inEdgeIds.addAll(vertex.inEdgesId.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(vertex.inEdgesId::get).filter(Objects::nonNull).forEach(inEdgeIds::addAll);
            }
        }

        return outEdgeIds.size() == 0 && inEdgeIds.size() == 0
                ? Collections.emptyIterator()
                : Stream.concat(outEdgeIds.stream(), inEdgeIds.stream())
                    .map(id -> ((AbstractTinkerGraph) vertex.graph()).edge(id))
                    .filter(v -> v != null)
                    .map(v -> (TinkerEdge) v).iterator();
    }

    public static Iterator<TinkerVertex> getVertices(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((TinkerEdge) edge).inVertex)));
                else if (edgeLabels.length == 1)
                    vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((TinkerEdge) edge).inVertex));
                else
                    Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((TinkerEdge) edge).inVertex));
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((TinkerEdge) edge).outVertex)));
                else if (edgeLabels.length == 1)
                    vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((TinkerEdge) edge).outVertex));
                else
                    Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((TinkerEdge) edge).outVertex));
            }
        }
        return (Iterator) vertices.iterator();
    }

    public static Iterator<TinkerVertex> getVerticesTx(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        final Set<Object> inEdgesIds = new HashSet<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdgesId != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdgesId.values().forEach(set -> set.forEach(edge -> inEdgesIds.add(edge)));
                else if (edgeLabels.length == 1)
                    vertex.outEdgesId.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> inEdgesIds.add(edge));
                else
                    Stream.of(edgeLabels).map(vertex.outEdgesId::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> inEdgesIds.add(edge));
            }
        }
        final Set<Object> outEdgesIds = new HashSet<>();
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdgesId != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdgesId.values().forEach(set -> set.forEach(edge -> outEdgesIds.add(edge)));
                else if (edgeLabels.length == 1)
                    vertex.inEdgesId.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> outEdgesIds.add(edge));
                else
                    Stream.of(edgeLabels).map(vertex.inEdgesId::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> outEdgesIds.add(edge));
            }
        }

        final List<Vertex> vertices = new ArrayList<>();
        if (inEdgesIds.size() != 0)
            vertex.graph().edges(inEdgesIds.toArray()).forEachRemaining(edge -> vertices.add(edge.inVertex()));
        if (outEdgesIds.size() != 0)
            vertex.graph().edges(outEdgesIds.toArray()).forEachRemaining(edge -> vertices.add(edge.outVertex()));

        return vertices.size() == 0
                ? Collections.emptyIterator()
                : vertices.stream().map(v -> (TinkerVertex) v).iterator();
    }

    /**
     * Allows direct access to a TinkerGraph's storage which can be helpful for advanced use cases.
     */
    public static Map<Object, Vertex> getVertices(final TinkerGraph graph) {
        return graph.vertices;
    }

    /**
     * Allows direct access to a TinkerGraph's storage which can be helpful for advanced use cases.
     */
    public static Map<Object, Edge> getEdges(final TinkerGraph graph) {
        return graph.edges;
    }

    // todo: move to SearchHelper?
    /**
     * Search for {@link Property}s attached to {@link Element}s of the supplied element type using the supplied
     * regex. This is a basic scan+filter operation, not a full text search against an index.
     */
    public static <E extends Element> Iterator<Property> search(final AbstractTinkerGraph graph, final String regex,
                                                                final Optional<Class<E>> type) {

        final Supplier<Iterator<Element>> vertices = () -> IteratorUtils.cast(graph.vertices());
        final Supplier<Iterator<Element>> edges = () -> IteratorUtils.cast(graph.edges());
        final Supplier<Iterator<Element>> vertexProperties =
                () -> IteratorUtils.flatMap(vertices.get(), v -> IteratorUtils.cast(v.properties()));

        Iterator it;
        if (!type.isPresent()) {
            it = IteratorUtils.concat(vertices.get(), edges.get(), vertexProperties.get());
        } else switch (type.get().getSimpleName()) {
            case "Edge":
                it = edges.get();
                break;
            case "Vertex":
                it = vertices.get();
                break;
            case "VertexProperty":
                it = vertexProperties.get();
                break;
            default:
                it = IteratorUtils.concat(vertices.get(), edges.get(), vertexProperties.get());
        }

        final Pattern pattern = Pattern.compile(regex);

        // get properties
        it = IteratorUtils.<Element, Property>flatMap(it, e -> IteratorUtils.cast(e.properties()));
        // filter by regex
        it = IteratorUtils.<Property>filter(it, p -> pattern.matcher(p.value().toString()).matches());

        return it;
    }

    /**
     * Search for {@link Property}s attached to any {@link Element} using the supplied regex. This
     * is a basic scan+filter operation, not a full text search against an index.
     */
    public static Iterator<Property> search(final TinkerGraph graph, final String regex) {
        return search(graph, regex, Optional.empty());
    }

}
