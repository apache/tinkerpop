/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DetachedElement<E> implements Element, Serializable, Attachable<E> {

    protected Object id;
    protected String label;
    protected Map<String, List<Property>> properties = null;

    protected DetachedElement() {

    }

    protected DetachedElement(final Element element) {
        this.id = element.id();
        try {
            this.label = element.label();
        } catch (final UnsupportedOperationException e) {   // ghetto.
            this.label = Vertex.DEFAULT_LABEL;
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
        return this.label;
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

    void internalSetId(final Object id) {
        this.id = id;
    }

    void inernalSetLabel(final String label) {
        this.label = label;
    }
}
