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
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A read-only edge facade holding the graph and an edge ordinal. Facades are created on demand and never cached.
 */
public final class CsrEdge implements Edge, CsrElement {

    private final CsrGraph graph;
    private final int ordinal;
    private Object id;

    CsrEdge(final CsrGraph graph, final int ordinal) {
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
            result = graph.snapshot().edgeId(ordinal);
            id = result;
        }
        return result;
    }

    @Override
    public String label() {
        return graph.snapshot().edgeLabel(ordinal);
    }

    @Override
    public CsrGraph graph() {
        return graph;
    }

    @Override
    public Vertex outVertex() {
        return new CsrVertex(graph, graph.snapshot().edgeOut(ordinal));
    }

    @Override
    public Vertex inVertex() {
        return new CsrVertex(graph, graph.snapshot().edgeIn(ordinal));
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        final CsrSnapshot snapshot = graph.snapshot();
        switch (direction) {
            case OUT:
                return new Pair(new CsrVertex(graph, snapshot.edgeOut(ordinal)), null);
            case IN:
                return new Pair(new CsrVertex(graph, snapshot.edgeIn(ordinal)), null);
            default:
                return new Pair(new CsrVertex(graph, snapshot.edgeOut(ordinal)),
                        new CsrVertex(graph, snapshot.edgeIn(ordinal)));
        }
    }

    @Override
    public <V> Property<V> property(final String key) {
        final int code = graph.edgeKeyCode(key);
        if (code < 0 || !graph.edgeColumn(code).isPresent(ordinal)) return Property.empty();
        return new CsrProperty<>(graph, ordinal, code);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        final int[] codes;
        if (propertyKeys.length == 0) {
            codes = null;
        } else {
            codes = new int[propertyKeys.length];
            for (int i = 0; i < codes.length; i++) codes[i] = graph.edgeKeyCode(propertyKeys[i]);
        }
        return new PropertyIterator<>(graph, ordinal, codes);
    }

    @Override
    public void remove() {
        throw Edge.Exceptions.edgeRemovalNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof CsrEdge) {
            final CsrEdge other = (CsrEdge) object;
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
        return StringFactory.edgeString(this);
    }

    /**
     * Up to two vertices, in order.
     */
    private static final class Pair implements Iterator<Vertex> {
        private Vertex first;
        private Vertex second;

        private Pair(final Vertex first, final Vertex second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean hasNext() {
            return first != null || second != null;
        }

        @Override
        public Vertex next() {
            if (first != null) {
                final Vertex result = first;
                first = null;
                return result;
            }
            if (second != null) {
                final Vertex result = second;
                second = null;
                return result;
            }
            throw new NoSuchElementException();
        }
    }

    /**
     * The present properties of the edge, for the given key codes (negative codes are skipped) or, when the codes are
     * null, for all keys in key-code order.
     */
    private static final class PropertyIterator<V> implements Iterator<Property<V>> {
        private final CsrGraph graph;
        private final int edge;
        private final int[] codes;
        private final int limit;
        private int index = 0;
        private int pending = -1;

        private PropertyIterator(final CsrGraph graph, final int edge, final int[] codes) {
            this.graph = graph;
            this.edge = edge;
            this.codes = codes;
            this.limit = codes == null ? graph.edgeKeyCount() : codes.length;
        }

        @Override
        public boolean hasNext() {
            while (pending < 0 && index < limit) {
                final int code = codes == null ? index : codes[index];
                index++;
                if (code >= 0 && graph.edgeColumn(code).isPresent(edge)) pending = code;
            }
            return pending >= 0;
        }

        @Override
        public Property<V> next() {
            if (!hasNext()) throw new NoSuchElementException();
            final int code = pending;
            pending = -1;
            return new CsrProperty<>(graph, edge, code);
        }
    }
}
