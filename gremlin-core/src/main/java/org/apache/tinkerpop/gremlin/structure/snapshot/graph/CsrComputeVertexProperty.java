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
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;

/**
 * A read-only facade for a computed vertex property.
 */
final class CsrComputeVertexProperty<V> implements VertexProperty<V> {

    private final CsrGraph graph;
    private final int vertex;
    private final String key;
    private final CsrComputePropertyColumn column;

    CsrComputeVertexProperty(final CsrGraph graph, final int vertex, final String key,
                             final CsrComputePropertyColumn column) {
        this.graph = graph;
        this.vertex = vertex;
        this.key = key;
        this.column = column;
    }

    @Override
    public Object id() {
        return key + ':' + vertex;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V value() {
        return (V) column.value(vertex);
    }

    @Override
    public boolean isPresent() {
        return column.isPresent(vertex);
    }

    @Override
    public Vertex element() {
        return graph.vertexAt(vertex);
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public <U> Property<U> property(final String key) {
        return Property.empty();
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        return Collections.emptyIterator();
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
}
