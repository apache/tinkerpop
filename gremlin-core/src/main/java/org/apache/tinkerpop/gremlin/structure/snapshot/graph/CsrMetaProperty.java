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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A read-only meta-property facade: a property of a vertex property, holding the vertex ordinal, the vertex property's
 * key code and ordinal, and the meta-property key code. The value, which may be null, is read from the snapshot on
 * demand.
 */
public final class CsrMetaProperty<V> implements Property<V> {

    private final CsrGraph graph;
    private final int vertex;
    private final int keyCode;
    private final long ordinal;
    private final int metaKeyCode;

    CsrMetaProperty(final CsrGraph graph, final int vertex, final int keyCode, final long ordinal,
                    final int metaKeyCode) {
        this.graph = graph;
        this.vertex = vertex;
        this.keyCode = keyCode;
        this.ordinal = ordinal;
        this.metaKeyCode = metaKeyCode;
    }

    @Override
    public String key() {
        return graph.metaKey(metaKeyCode);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V value() throws NoSuchElementException {
        return (V) graph.snapshot().metaPropertyValue(keyCode, metaKeyCode, ordinal);
    }

    /**
     * Always true: a facade is only created for a meta-property that exists, even when its value is null.
     */
    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Element element() {
        return new CsrVertexProperty<>(graph, vertex, keyCode, ordinal);
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
