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

import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ReferenceElement<E extends Element> implements Element, Serializable, Attachable<E> {

    protected Object id;
    protected String label;

    /**
     * Multi-label storage. When non-null, takes precedence over the single {@link #label} field.
     * Only populated when the source element has more than one label.
     */
    protected Set<String> elementLabels;

    protected ReferenceElement() {

    }

    protected ReferenceElement(final Object id, final String label) {
        this.id = id;
        this.label = label;
    }

    public ReferenceElement(final Element element) {
        this.id = element.id();
        try {
            //Exception creation takes too much time, return default values for known types
            if (element instanceof ComputerGraph.ComputerAdjacentVertex) {
                this.label = Vertex.DEFAULT_LABEL;
            } else {
                this.label = element.label();
            }
        } catch (final UnsupportedOperationException e) {
            if (element instanceof Vertex)
                this.label = Vertex.DEFAULT_LABEL;
            else if (element instanceof Edge)
                this.label = Edge.DEFAULT_LABEL;
            else
                this.label = VertexProperty.DEFAULT_LABEL;
        }
        // Capture all labels from the source element for multi-label support.
        try {
            final Set<String> srcLabels = element.labels();
            if (srcLabels.size() > 1) {
                this.elementLabels = new LinkedHashSet<>(srcLabels);
            }
        } catch (UnsupportedOperationException e) {
            // Adjacent vertices in graph computer context may not support labels()
        }
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        if (this.elementLabels != null && !this.elementLabels.isEmpty()) {
            return this.elementLabels.iterator().next();
        }
        return this.label;
    }

    @Override
    public Set<String> labels() {
        if (this.elementLabels != null) {
            return Collections.unmodifiableSet(this.elementLabels);
        }
        return this.label != null ? Collections.singleton(this.label) : Collections.emptySet();
    }

    /**
     * Sets multi-label storage directly. Used by constructors and deserialization.
     */
    protected void setElementLabels(final Set<String> labels) {
        if (labels != null && !labels.isEmpty()) {
            this.elementLabels = new LinkedHashSet<>(labels);
            this.label = labels.iterator().next();
        }
    }

    @Override
    public Graph graph() {
        return EmptyGraph.instance();
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return ElementHelper.areEqual(this, other);
    }

    public E get() {
        return (E) this;
    }
}
