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
package org.apache.tinkerpop.gremlin.structure.util.detached;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DetachedElement<E> implements Element, Serializable, Attachable<E> {

    protected Object id;
    protected String label;
    protected Map<String, List<Property>> properties = null;

    /**
     * Multi-label storage. When non-null, takes precedence over the single {@link #label} field.
     * Only populated when the source element has more than one label.
     */
    protected Set<String> elementLabels;

    protected DetachedElement() {

    }

    protected DetachedElement(final Element element) {
        this.id = element.id();
        try {
            this.label = element.label();
        } catch (final UnsupportedOperationException e) {   // ghetto.
            this.label = Vertex.DEFAULT_LABEL;
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

    protected DetachedElement(final Object id, final String label) {
        this.id = id;
        this.label = label;
    }

    @Override
    public Graph graph() {
        return EmptyGraph.instance();
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
        return this.label != null ? this.label : "";
    }

    @Override
    public Set<String> labels() {
        if (this.elementLabels != null) {
            return Collections.unmodifiableSet(this.elementLabels);
        }
        return this.label != null ? Collections.singleton(this.label) : Collections.emptySet();
    }

    /**
     * Sets multi-label storage directly. Used by builders and deserialization.
     */
    protected void setElementLabels(final Set<String> labels) {
        if (labels != null && !labels.isEmpty()) {
            this.elementLabels = new LinkedHashSet<>(labels);
            this.label = labels.iterator().next();
        }
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> Property<V> property(final String key) {
        return null != this.properties && this.properties.containsKey(key) ? this.properties.get(key).get(0) : Property.empty();
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        return null == this.properties ?
                Collections.emptyIterator() :
                (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).flatMap(entry -> entry.getValue().stream()).iterator();
    }

    public E get() {
        return (E) this;
    }

    abstract void internalAddProperty(final Property p);
}
