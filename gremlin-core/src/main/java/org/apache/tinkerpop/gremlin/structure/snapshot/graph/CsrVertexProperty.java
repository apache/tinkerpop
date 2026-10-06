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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A read-only vertex property facade holding a vertex ordinal, a property key code and the vertex-property ordinal
 * within that key. The value, which may be null, and the meta-properties are read from the snapshot on demand.
 */
public final class CsrVertexProperty<V> implements VertexProperty<V> {

    private final CsrGraph graph;
    private final int vertex;
    private final int keyCode;
    private final long ordinal;

    CsrVertexProperty(final CsrGraph graph, final int vertex, final int keyCode, final long ordinal) {
        this.graph = graph;
        this.vertex = vertex;
        this.keyCode = keyCode;
        this.ordinal = ordinal;
    }

    /**
     * The ordinal of the owning vertex.
     */
    public int vertexOrdinal() {
        return vertex;
    }

    /**
     * The code of the vertex property key.
     */
    public int keyCode() {
        return keyCode;
    }

    /**
     * The vertex-property ordinal within the key, see {@code CsrSnapshot#vertexPropertyStart(int, int)}.
     */
    public long propertyOrdinal() {
        return ordinal;
    }

    @Override
    public Object id() {
        return graph.snapshot().vertexPropertyIdentifier(keyCode, ordinal);
    }

    @Override
    public String key() {
        return graph.vertexKey(keyCode);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V value() throws NoSuchElementException {
        return (V) graph.snapshot().vertexPropertyValue(keyCode, ordinal);
    }

    /**
     * Always true: a facade is only created for a vertex property that exists, even when its value is null.
     */
    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return new CsrVertex(graph, vertex);
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public <U> Property<U> property(final String key) {
        final int metaCode = graph.metaKeyCode(key);
        if (metaCode < 0 || !graph.snapshot().hasMetaProperty(keyCode, metaCode, ordinal)) return Property.empty();
        return new CsrMetaProperty<>(graph, vertex, keyCode, ordinal, metaCode);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        final int[] present = graph.snapshot().metaKeyCodes(keyCode);
        if (present.length == 0) return Collections.emptyIterator();
        final int[] codes;
        if (propertyKeys.length == 0) {
            codes = present;
        } else {
            codes = new int[propertyKeys.length];
            for (int i = 0; i < codes.length; i++) codes[i] = graph.metaKeyCode(propertyKeys[i]);
        }
        return new MetaIterator<>(graph, vertex, keyCode, ordinal, codes);
    }

    @Override
    public void remove() {
        throw Property.Exceptions.propertyRemovalNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    /**
     * The meta-properties of a vertex property for the given meta key codes (negative codes are skipped), in the
     * order of the codes.
     */
    private static final class MetaIterator<U> implements Iterator<Property<U>> {
        private final CsrGraph graph;
        private final CsrSnapshot snapshot;
        private final int vertex;
        private final int keyCode;
        private final long ordinal;
        private final int[] codes;
        private int index = 0;
        private int pending = -1;

        private MetaIterator(final CsrGraph graph, final int vertex, final int keyCode, final long ordinal,
                             final int[] codes) {
            this.graph = graph;
            this.snapshot = graph.snapshot();
            this.vertex = vertex;
            this.keyCode = keyCode;
            this.ordinal = ordinal;
            this.codes = codes;
        }

        @Override
        public boolean hasNext() {
            while (pending < 0 && index < codes.length) {
                final int code = codes[index++];
                if (code >= 0 && snapshot.hasMetaProperty(keyCode, code, ordinal)) pending = code;
            }
            return pending >= 0;
        }

        @Override
        public Property<U> next() {
            if (!hasNext()) throw new NoSuchElementException();
            final int code = pending;
            pending = -1;
            return new CsrMetaProperty<>(graph, vertex, keyCode, ordinal, code);
        }
    }
}
