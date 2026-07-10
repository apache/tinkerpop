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
package org.apache.tinkerpop.gremlin.structure.util.reference;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceVertex extends ReferenceElement<Vertex> implements Vertex {

    protected Set<String> vertexLabels;

    private ReferenceVertex() {

    }

    public ReferenceVertex(final Object id) {
        this(id, Vertex.DEFAULT_LABEL);
    }

    public ReferenceVertex(final Object id, final String label) {
        super(id, label);
        this.vertexLabels = toLabelSet(label);
    }

    public ReferenceVertex(final Object id, final Set<String> labels) {
        super(id, labels != null && !labels.isEmpty() ? labels.iterator().next() : Vertex.DEFAULT_LABEL);
        this.vertexLabels = (labels != null && !labels.isEmpty()) ? new LinkedHashSet<>(labels) : toLabelSet(Vertex.DEFAULT_LABEL);
    }

    public ReferenceVertex(final Vertex vertex) {
        super(vertex);
        Set<String> resolved;
        try {
            resolved = new LinkedHashSet<>(vertex.labels());
        } catch (final UnsupportedOperationException e) {
            // adjacent vertices in graph computer context may not support labels(); seed the set from the single
            // label the superclass constructor resolved (e.g. the default label for adjacent vertices)
            resolved = toLabelSet(super.label());
        }
        this.vertexLabels = resolved;
    }

    private static Set<String> toLabelSet(final String label) {
        final Set<String> set = new LinkedHashSet<>();
        if (label != null) set.add(label);
        return set;
    }

    @Override
    public String label() {
        return (this.vertexLabels == null || this.vertexLabels.isEmpty()) ? "" : this.vertexLabels.iterator().next();
    }

    @Override
    public Set<String> labels() {
        return this.vertexLabels == null ? Collections.emptySet() : Collections.unmodifiableSet(this.vertexLabels);
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw Vertex.Exceptions.edgeAdditionsNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return Collections.emptyIterator();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return Collections.emptyIterator();
    }

    @Override
    public void remove() {
        throw Vertex.Exceptions.vertexRemovalNotSupported();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
