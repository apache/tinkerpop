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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Writes traversal results and graph state as strings that compare equal across two copies of the same graph. An
 * element that existed before the traversal ran is written by its id, which both copies share. An element the
 * traversal created is written by its content, its label, properties and, for an edge, its endpoints, because the
 * two sides may create elements in a different order and so give them different ids. A graph returned as a result,
 * such as one built by {@code subgraph()}, is written as its state. Collections are written sorted, as order is
 * checked by the scoring pass, and numbers are rounded to ten significant digits so that sums of doubles taken in a
 * different order still match.
 */
final class Canonical {
    private static final MathContext PRECISION = new MathContext(10);

    private final Set<Object> vertexIds = new HashSet<>();
    private final Set<Object> edgeIds = new HashSet<>();

    /**
     * Records the elements of the graph as it is before the traversal runs.
     */
    Canonical(final Graph graph) {
        graph.vertices().forEachRemaining(v -> vertexIds.add(v.id()));
        graph.edges().forEachRemaining(e -> edgeIds.add(e.id()));
    }

    /**
     * The graph as a sorted list of one entry per vertex and edge.
     */
    List<String> state(final Graph graph) {
        final List<String> entries = new ArrayList<>();
        graph.vertices().forEachRemaining(v -> entries.add(vertexRef(v) + vertexContent(v)));
        graph.edges().forEachRemaining(e -> entries.add(
                (edgeIds.contains(e.id()) ? "e[" + e.id() + "]" : "e*") + edgeContent(e)));
        entries.sort(null);
        return entries;
    }

    /**
     * A traversal result as a sorted list of its items.
     */
    List<String> result(final Object result) {
        final List<String> items = new ArrayList<>();
        if (result instanceof List)
            ((List<?>) result).forEach(o -> items.add(of(o)));
        else
            items.add(of(result));
        items.sort(null);
        return items;
    }

    String of(final Object o) {
        if (null == o) return "null";
        if (o instanceof Vertex) {
            final Vertex v = (Vertex) o;
            return vertexIds.contains(v.id()) ? vertexRef(v) : vertexRef(v) + vertexContent(v);
        }
        if (o instanceof Edge) {
            final Edge e = (Edge) o;
            return edgeIds.contains(e.id()) ? "e[" + e.id() + "]" : "e*" + edgeContent(e);
        }
        if (o instanceof VertexProperty) {
            final VertexProperty<?> vp = (VertexProperty<?>) o;
            return "vp[" + vp.key() + "=" + of(vp.value()) + metaProperties(vp) + "]";
        }
        if (o instanceof Property) {
            final Property<?> p = (Property<?>) o;
            return "p[" + p.key() + "=" + of(p.value()) + "]";
        }
        if (o instanceof Graph)
            return state((Graph) o).stream().collect(Collectors.joining(",", "graph[", "]"));
        if (o instanceof Path)
            return ((Path) o).objects().stream().map(this::of).collect(Collectors.joining(",", "path[", "]"));
        if (o instanceof Map)
            return ((Map<?, ?>) o).entrySet().stream().map(this::entry).sorted().collect(Collectors.joining(",", "{", "}"));
        if (o instanceof Map.Entry)
            return "{" + entry((Map.Entry<?, ?>) o) + "}";
        if (o instanceof Collection)
            return ((Collection<?>) o).stream().map(this::of).sorted().collect(Collectors.joining(",", "[", "]"));
        if (o instanceof Number) return number((Number) o);
        if (o instanceof String) return "\"" + o + "\"";
        return o.toString();
    }

    private String entry(final Map.Entry<?, ?> e) {
        return of(e.getKey()) + "=" + of(e.getValue());
    }

    private String vertexRef(final Vertex v) {
        return vertexIds.contains(v.id()) ? "v[" + v.id() + "]" : "v*";
    }

    private String vertexContent(final Vertex v) {
        final List<String> properties = new ArrayList<>();
        v.properties().forEachRemaining(vp -> properties.add(vp.key() + "=" + of(vp.value()) + metaProperties(vp)));
        properties.sort(null);
        return v.label() + properties;
    }

    private String edgeContent(final Edge e) {
        final List<String> properties = new ArrayList<>();
        e.properties().forEachRemaining(p -> properties.add(p.key() + "=" + of(p.value())));
        properties.sort(null);
        return e.label() + "(" + endpoint(e.outVertex()) + "->" + endpoint(e.inVertex()) + ")" + properties;
    }

    private String endpoint(final Vertex v) {
        return vertexIds.contains(v.id()) ? vertexRef(v) : vertexRef(v) + vertexContent(v);
    }

    private String metaProperties(final VertexProperty<?> vp) {
        final List<String> meta = new ArrayList<>();
        vp.properties().forEachRemaining(p -> meta.add(p.key() + "=" + of(p.value())));
        if (meta.isEmpty()) return "";
        meta.sort(null);
        return meta.toString();
    }

    private static String number(final Number n) {
        if (n instanceof Double && (((Double) n).isNaN() || ((Double) n).isInfinite())) return n.toString();
        if (n instanceof Float && (((Float) n).isNaN() || ((Float) n).isInfinite())) return n.toString();
        return new BigDecimal(n.toString()).round(PRECISION).stripTrailingZeros().toPlainString();
    }
}
