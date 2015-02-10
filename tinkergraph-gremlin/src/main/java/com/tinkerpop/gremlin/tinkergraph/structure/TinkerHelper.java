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
package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerHelper {

    protected final synchronized static long getNextId(final TinkerGraph graph) {
        return Stream.generate(() -> (++graph.currentId)).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
    }

    protected static Edge addEdge(final TinkerGraph graph, final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        final Edge edge;
        if (null != idValue) {
            if (graph.edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = TinkerHelper.getNextId(graph);
        }

        edge = new TinkerEdge(idValue, outVertex, label, inVertex, graph);
        ElementHelper.attachProperties(edge, keyValues);
        graph.edges.put(edge.id(), edge);
        TinkerHelper.addOutEdge(outVertex, label, edge);
        TinkerHelper.addInEdge(inVertex, label, edge);
        return edge;

    }

    protected static void addOutEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        Set<Edge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    protected static void addInEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        Set<Edge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.inEdges.put(label, edges);
        }
        edges.add(edge);
    }

    public static List<TinkerVertex> queryVertexIndex(final TinkerGraph graph, final String key, final Object value) {
        return graph.vertexIndex.get(key, value);
    }

    public static List<TinkerEdge> queryEdgeIndex(final TinkerGraph graph, final String key, final Object value) {
        return graph.edgeIndex.get(key, value);
    }

    public static boolean inComputerMode(final TinkerGraph graph) {
        return null != graph.graphView;
    }

    public static TinkerGraphView createGraphView(final TinkerGraph graph, final GraphComputer.Isolation isolation, final Set<String> computeKeys) {
        return graph.graphView = new TinkerGraphView(isolation, computeKeys);
    }

    public static Map<String, List<Property>> getProperties(final TinkerElement element) {
        return element.properties;
    }

    public static final Iterator<TinkerEdge> getEdges(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Edge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.outEdges.values().forEach(edges::addAll);
            else if (edgeLabels.length == 1)
                edges.addAll(vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
            else
                Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.inEdges.values().forEach(edges::addAll);
            else if (edgeLabels.length == 1)
                edges.addAll(vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
            else
                Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
        }
        return (Iterator) edges.iterator();
    }

    public static final Iterator<TinkerVertex> getVertices(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.outEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((TinkerEdge) edge).inVertex)));
            else if (edgeLabels.length == 1)
                vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((TinkerEdge) edge).inVertex));
            else
                Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((TinkerEdge) edge).inVertex));
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.inEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((TinkerEdge) edge).outVertex)));
            else if (edgeLabels.length == 1)
                vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((TinkerEdge) edge).outVertex));
            else
                Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((TinkerEdge) edge).outVertex));
        }
        return (Iterator) vertices.iterator();
    }
}
