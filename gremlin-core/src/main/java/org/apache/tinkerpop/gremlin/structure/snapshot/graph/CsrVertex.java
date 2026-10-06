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
package org.apache.tinkerpop.gremlin.structure.snapshot.graph;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A read-only vertex facade holding the graph and a vertex ordinal. Facades are created on demand and never cached.
 */
public final class CsrVertex implements Vertex, CsrElement {

    private final CsrGraph graph;
    private final int ordinal;
    private Object id;

    CsrVertex(final CsrGraph graph, final int ordinal) {
        this.graph = graph;
        this.ordinal = ordinal;
    }

    @Override
    public int ordinal() {
        return ordinal;
    }

    @Override
    public Object id() {
        Object result = id;
        if (result == null) {
            result = graph.snapshot().vertexId(ordinal);
            id = result;
        }
        return result;
    }

    /**
     * The first label in source order, or the empty string when the vertex has none.
     */
    @Override
    public String label() {
        return graph.snapshot().vertexLabel(ordinal);
    }

    @Override
    public Set<String> labels() {
        final CsrSnapshot snapshot = graph.snapshot();
        final int count = snapshot.vertexLabelCount(ordinal);
        if (count == 0) return Collections.emptySet();
        final List<String> dictionary = snapshot.vertexLabels();
        if (count == 1) return Collections.singleton(dictionary.get(snapshot.vertexLabelCodeAt(ordinal, 0)));
        final Set<String> labels = new LinkedHashSet<>();
        for (int i = 0; i < count; i++) labels.add(dictionary.get(snapshot.vertexLabelCodeAt(ordinal, i)));
        return Collections.unmodifiableSet(labels);
    }

    @Override
    public CsrGraph graph() {
        return graph;
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw Vertex.Exceptions.edgeAdditionsNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key,
                                          final V value, final Object... keyValues) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        final int code = graph.vertexKeyCode(key);
        if (code < 0) return VertexProperty.empty();
        final CsrSnapshot snapshot = graph.snapshot();
        final long start = snapshot.vertexPropertyStart(code, ordinal);
        final long end = snapshot.vertexPropertyEnd(code, ordinal);
        if (end <= start) return VertexProperty.empty();
        if (end - start > 1) throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
        return new CsrVertexProperty<>(graph, ordinal, code, start);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        final int[] codes;
        if (propertyKeys.length == 0) {
            codes = null;
        } else {
            codes = new int[propertyKeys.length];
            for (int i = 0; i < codes.length; i++) codes[i] = graph.vertexKeyCode(propertyKeys[i]);
        }
        return new PropertyIterator<>(graph, ordinal, codes);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        final int[] labelCodes = labelCodes(edgeLabels);
        if (labelCodes != null && labelCodes.length == 0) return Collections.emptyIterator();
        return new AdjacencyIterator<>(graph, ordinal, direction, labelCodes, true);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        final int[] labelCodes = labelCodes(edgeLabels);
        if (labelCodes != null && labelCodes.length == 0) return Collections.emptyIterator();
        return new AdjacencyIterator<>(graph, ordinal, direction, labelCodes, false);
    }

    /**
     * Resolves edge labels to codes once: null for no filter, an empty array when none of the labels exist.
     */
    private int[] labelCodes(final String[] edgeLabels) {
        if (edgeLabels.length == 0) return null;
        final CsrSnapshot snapshot = graph.snapshot();
        int[] codes = new int[edgeLabels.length];
        int n = 0;
        for (final String label : edgeLabels) {
            final int code = snapshot.edgeLabelCodeOf(label);
            if (code >= 0) codes[n++] = code;
        }
        if (n != codes.length) {
            final int[] trimmed = new int[n];
            System.arraycopy(codes, 0, trimmed, 0, n);
            codes = trimmed;
        }
        return codes;
    }

    @Override
    public void remove() {
        throw Vertex.Exceptions.vertexRemovalNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof CsrVertex) {
            final CsrVertex other = (CsrVertex) object;
            if (other.graph.snapshot() == graph.snapshot()) return other.ordinal == ordinal;
        }
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return id().hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    /**
     * Iterates the out and/or in adjacency range of a vertex in position order, out first, and produces either edges
     * or adjacent vertices.
     */
    private static final class AdjacencyIterator<T> implements Iterator<T> {
        private final CsrGraph graph;
        private final CsrSnapshot snapshot;
        private final int[] labelCodes;
        private final boolean edges;

        // the ranges still to visit: the current one, then the pending in range
        private boolean outDirection;
        private long position;
        private long end;
        private boolean inPending;
        private long inStart;
        private long inEnd;

        // lookahead
        private int nextEdge = -1;
        private int nextNeighbor = -1;

        private AdjacencyIterator(final CsrGraph graph, final int vertex, final Direction direction,
                                  final int[] labelCodes, final boolean edges) {
            this.graph = graph;
            this.snapshot = graph.snapshot();
            this.labelCodes = labelCodes;
            this.edges = edges;
            switch (direction) {
                case OUT:
                    outDirection = true;
                    position = snapshot.outStart(vertex);
                    end = snapshot.outEnd(vertex);
                    break;
                case IN:
                    outDirection = false;
                    position = snapshot.inStart(vertex);
                    end = snapshot.inEnd(vertex);
                    break;
                default:
                    outDirection = true;
                    position = snapshot.outStart(vertex);
                    end = snapshot.outEnd(vertex);
                    inPending = true;
                    inStart = snapshot.inStart(vertex);
                    inEnd = snapshot.inEnd(vertex);
                    break;
            }
        }

        @Override
        public boolean hasNext() {
            if (nextEdge >= 0) return true;
            while (true) {
                while (position >= end) {
                    if (!inPending) return false;
                    inPending = false;
                    outDirection = false;
                    position = inStart;
                    end = inEnd;
                }
                final long current = position++;
                final int edge = outDirection ? snapshot.outEdge(current) : snapshot.inEdge(current);
                if (labelCodes != null && !matches(snapshot.edgeLabelCode(edge))) continue;
                nextEdge = edge;
                if (!edges) nextNeighbor = outDirection ? snapshot.outNeighbor(current) : snapshot.inNeighbor(current);
                return true;
            }
        }

        private boolean matches(final int code) {
            for (final int wanted : labelCodes) {
                if (wanted == code) return true;
            }
            return false;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            final T result = edges ? (T) new CsrEdge(graph, nextEdge) : (T) new CsrVertex(graph, nextNeighbor);
            nextEdge = -1;
            return result;
        }
    }

    /**
     * The vertex properties of the vertex, for the given key codes in argument order (negative codes are skipped) or,
     * when the codes are null, for all keys in key-code order. All vertex properties of one key are returned before
     * those of the next, in source order.
     */
    private static final class PropertyIterator<V> implements Iterator<VertexProperty<V>> {
        private final CsrGraph graph;
        private final CsrSnapshot snapshot;
        private final int vertex;
        private final int[] codes;
        private final int limit;
        private int index = 0;
        private int code = -1;
        private long position = 0;
        private long end = 0;

        private PropertyIterator(final CsrGraph graph, final int vertex, final int[] codes) {
            this.graph = graph;
            this.snapshot = graph.snapshot();
            this.vertex = vertex;
            this.codes = codes;
            this.limit = codes == null ? graph.vertexKeyCount() : codes.length;
        }

        @Override
        public boolean hasNext() {
            while (position >= end && index < limit) {
                final int next = codes == null ? index : codes[index];
                index++;
                if (next < 0) continue;
                code = next;
                position = snapshot.vertexPropertyStart(next, vertex);
                end = snapshot.vertexPropertyEnd(next, vertex);
            }
            return position < end;
        }

        @Override
        public VertexProperty<V> next() {
            if (!hasNext()) throw new NoSuchElementException();
            return new CsrVertexProperty<>(graph, vertex, code, position++);
        }
    }
}
