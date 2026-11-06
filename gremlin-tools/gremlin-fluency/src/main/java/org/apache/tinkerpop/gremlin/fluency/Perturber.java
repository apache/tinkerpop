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
package org.apache.tinkerpop.gremlin.fluency;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerMemoryGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Adds seeded random elements to a graph so that a traversal which only matches its reference because of the data
 * stops matching. Additions are drawn from the graph's own schema: vertices take an existing vertex's label and some
 * of the keys seen on that label, edges take an existing edge's label and endpoint labels, and every value is one
 * already used for that key somewhere in the graph, so a country vertex may get an airport's code. No added edge
 * connects a vertex to itself. A graph that follows a {@link DomainSchema} also keeps to what the schema understands
 * about its relations: an added edge never gives a vertex a second parent in a hierarchy or a second container, and
 * never closes a cycle in a hierarchy, so a traversal written against the schema need not defend against data the
 * schema rules out. Nothing is removed or changed, so ids and the parameters that refer to them still resolve. The
 * same seed on the same graph always adds the same elements with the same ids.
 */
final class Perturber {
    /**
     * The share of a graph's vertex and edge counts that is added.
     */
    static final double SHARE = 0.10;
    static final int MIN_VERTICES = 3;
    static final int MIN_EDGES = 6;

    /**
     * The chance that an added element gets each key its template element's label carries.
     */
    private static final double KEY_CHANCE = 0.7;

    /**
     * How many endpoint pairs are drawn for an added edge before it is left out because none is allowed.
     */
    private static final int ENDPOINT_ATTEMPTS = 20;

    private static final Comparator<Element> BY_ID = Comparator.comparing(e -> String.valueOf(e.id()));

    private Perturber() {}

    /**
     * Copies a TinkerGraph with the same configuration, ids, labels, properties and meta-properties.
     */
    static AbstractTinkerGraph copy(final AbstractTinkerGraph source) {
        final AbstractTinkerGraph copy = FluencyWorld.registerTestServices(TinkerMemoryGraph.open(source.configuration()));
        final Map<Object, Vertex> vertices = new HashMap<>();
        for (Vertex v : sorted(source.vertices())) {
            final Vertex c = copy.addVertex(T.id, v.id(), T.label, v.label());
            v.properties().forEachRemaining(vp -> {
                final List<Object> keyValues = new ArrayList<>();
                keyValues.add(T.id);
                keyValues.add(vp.id());
                vp.properties().forEachRemaining(p -> {
                    keyValues.add(p.key());
                    keyValues.add(p.value());
                });
                c.property(VertexProperty.Cardinality.list, vp.key(), vp.value(), keyValues.toArray());
            });
            vertices.put(v.id(), c);
        }
        for (Edge e : sorted(source.edges())) {
            final List<Object> keyValues = new ArrayList<>();
            keyValues.add(T.id);
            keyValues.add(e.id());
            e.properties().forEachRemaining(p -> {
                keyValues.add(p.key());
                keyValues.add(p.value());
            });
            vertices.get(e.outVertex().id()).addEdge(e.label(), vertices.get(e.inVertex().id()), keyValues.toArray());
        }
        return copy;
    }

    /**
     * Adds about {@link #SHARE} of the graph's vertex and edge counts, at least {@link #MIN_VERTICES} vertices and
     * {@link #MIN_EDGES} edges. An empty graph is left alone, as it has no schema to draw from.
     *
     * @throws IllegalStateException if the additions break the graph's domain schema, which is a fault in the
     *                               perturber rather than in the scenario
     */
    static void perturb(final Graph graph, final long seed) {
        final List<Vertex> vertices = sorted(graph.vertices());
        final List<Edge> edges = sorted(graph.edges());
        if (vertices.isEmpty()) return;

        final DomainSchema schema = DomainSchema.forGraph(graph);
        final List<String> before = null == schema ? null : schema.violations(graph);

        final Random random = new Random(seed);
        final Map<String, TreeSet<String>> vertexKeys = new TreeMap<>();
        final Map<String, List<Object>> vertexValues = new HashMap<>();
        final Map<String, List<Vertex>> byLabel = new TreeMap<>();
        for (Vertex v : vertices) {
            byLabel.computeIfAbsent(v.label(), k -> new ArrayList<>()).add(v);
            final TreeSet<String> keys = vertexKeys.computeIfAbsent(v.label(), k -> new TreeSet<>());
            v.properties().forEachRemaining(vp -> {
                keys.add(vp.key());
                vertexValues.computeIfAbsent(vp.key(), k -> new ArrayList<>()).add(vp.value());
            });
        }

        final int vertexCount = Math.max(MIN_VERTICES, (int) Math.round(vertices.size() * SHARE));
        for (int i = 0; i < vertexCount; i++) {
            final String label = vertices.get(random.nextInt(vertices.size())).label();
            final List<Object> keyValues = new ArrayList<>();
            keyValues.add(T.label);
            keyValues.add(label);
            addValues(random, vertexKeys.get(label), vertexValues, keyValues);
            byLabel.get(label).add(graph.addVertex(keyValues.toArray()));
        }

        if (!edges.isEmpty()) addEdges(edges, byLabel, schema, random);

        if (null != schema) {
            final List<String> added = new ArrayList<>(schema.violations(graph));
            added.removeAll(before);
            if (!added.isEmpty())
                throw new IllegalStateException(String.format("Perturbing with seed %d broke the %s schema: %s",
                        seed, schema.getName(), added));
        }
    }

    private static void addEdges(final List<Edge> edges, final Map<String, List<Vertex>> byLabel,
                                 final DomainSchema schema, final Random random) {
        final Map<String, TreeSet<String>> edgeKeys = new TreeMap<>();
        final Map<String, List<Object>> edgeValues = new HashMap<>();
        for (Edge e : edges) {
            final TreeSet<String> keys = edgeKeys.computeIfAbsent(e.label(), k -> new TreeSet<>());
            e.properties().forEachRemaining(p -> {
                keys.add(p.key());
                edgeValues.computeIfAbsent(p.key(), k -> new ArrayList<>()).add(p.value());
            });
        }

        final int edgeCount = Math.max(MIN_EDGES, (int) Math.round(edges.size() * SHARE));
        for (int i = 0; i < edgeCount; i++) {
            final Edge template = edges.get(random.nextInt(edges.size()));
            final List<Vertex> outs = byLabel.get(template.outVertex().label());
            final List<Vertex> ins = byLabel.get(template.inVertex().label());
            for (int attempt = 0; attempt < ENDPOINT_ATTEMPTS; attempt++) {
                final Vertex out = outs.get(random.nextInt(outs.size()));
                final Vertex in = ins.get(random.nextInt(ins.size()));
                if (!allowed(schema, template.label(), out, in)) continue;

                final List<Object> keyValues = new ArrayList<>();
                addValues(random, edgeKeys.get(template.label()), edgeValues, keyValues);
                out.addEdge(template.label(), in, keyValues.toArray());
                break;
            }
        }
    }

    private static boolean allowed(final DomainSchema schema, final String label, final Vertex out, final Vertex in) {
        if (out.id().equals(in.id())) return false;
        final DomainSchema.EdgeType type = null == schema ? null : schema.getEdgeType(label);
        if (null == type) return true;
        switch (type.shape) {
            case HIERARCHY:
                return !out.edges(Direction.OUT, label).hasNext() && !DomainSchema.reaches(in, out, label);
            case CONTAINMENT:
                return !in.edges(Direction.IN, label).hasNext();
            default:
                return true;
        }
    }

    private static void addValues(final Random random, final TreeSet<String> keys, final Map<String, List<Object>> values,
                                  final List<Object> keyValues) {
        for (String key : keys) {
            if (random.nextDouble() >= KEY_CHANCE) continue;
            final List<Object> pool = values.get(key);
            keyValues.add(key);
            keyValues.add(pool.get(random.nextInt(pool.size())));
        }
    }

    private static <E extends Element> List<E> sorted(final Iterator<E> elements) {
        final List<E> list = IteratorUtils.list(elements);
        list.sort(BY_ID);
        return list;
    }
}
