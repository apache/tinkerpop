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
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceEdge extends ReferenceElement<Edge> implements Edge {

    private ReferenceVertex inVertex;
    private ReferenceVertex outVertex;
    private Set<String> edgeLabels;

    private ReferenceEdge() {

    }

    public ReferenceEdge(final Edge edge) {
        super(edge);
        this.inVertex = new ReferenceVertex(edge.inVertex());
        this.outVertex = new ReferenceVertex(edge.outVertex());
        // Capture all labels from the source edge for multi-label support.
        final Set<String> srcLabels = edge.labels();
        if (srcLabels.size() > 1) {
            this.edgeLabels = new LinkedHashSet<>(srcLabels);
        }
    }

    public ReferenceEdge(final Object id, final String label, final ReferenceVertex inVertex, final ReferenceVertex outVertex) {
        super(id, label);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }

    public ReferenceEdge(final Object id, final Set<String> labels, final ReferenceVertex inVertex, final ReferenceVertex outVertex) {
        super(id, labels != null && !labels.isEmpty() ? labels.iterator().next() : Edge.DEFAULT_LABEL);
        this.edgeLabels = labels != null ? new LinkedHashSet<>(labels) : null;
        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }

    @Override
    public Set<String> labels() {
        if (this.edgeLabels != null) {
            return Collections.unmodifiableSet(this.edgeLabels);
        }
        return this.label != null ? Collections.singleton(this.label) : Collections.emptySet();
    }

    @Override
    public String label() {
        if (this.edgeLabels != null && !this.edgeLabels.isEmpty()) {
            return this.edgeLabels.iterator().next();
        }
        return this.label != null ? this.label : Edge.DEFAULT_LABEL;
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public void remove() {
        throw Edge.Exceptions.edgeRemovalNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return IteratorUtils.of(this.outVertex);
        else if (direction.equals(Direction.IN))
            return IteratorUtils.of(this.inVertex);
        else
            return IteratorUtils.of(this.outVertex, this.inVertex);
    }

    @Override
    public Vertex inVertex() {
        return this.inVertex;
    }

    @Override
    public Vertex outVertex() {
        return this.outVertex;
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return Collections.emptyIterator();
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
