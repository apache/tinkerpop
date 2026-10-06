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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A read-only edge property facade holding an edge ordinal and a property key code. The value is decoded from the
 * column on every {@link #value()} call.
 */
public final class CsrProperty<V> implements Property<V> {

    private final CsrGraph graph;
    private final int edge;
    private final int keyCode;

    CsrProperty(final CsrGraph graph, final int edge, final int keyCode) {
        this.graph = graph;
        this.edge = edge;
        this.keyCode = keyCode;
    }

    /**
     * The ordinal of the owning edge.
     */
    public int edgeOrdinal() {
        return edge;
    }

    /**
     * The code of the edge property key.
     */
    public int keyCode() {
        return keyCode;
    }

    @Override
    public String key() {
        return graph.edgeKey(keyCode);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V value() throws NoSuchElementException {
        return (V) graph.edgeColumn(keyCode).get(edge);
    }

    @Override
    public boolean isPresent() {
        return graph.edgeColumn(keyCode).isPresent(edge);
    }

    @Override
    public Edge element() {
        return new CsrEdge(graph, edge);
    }

    @Override
    public void remove() {
        throw Property.Exceptions.propertyRemovalNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) return true;
        if (!(object instanceof Property)) return false;
        final Property<?> other = (Property<?>) object;
        if (!other.isPresent()) return false;
        return key().equals(other.key()) && Objects.equals(value(), other.value());
    }

    @Override
    public int hashCode() {
        return key().hashCode() + Objects.hashCode(value());
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}
