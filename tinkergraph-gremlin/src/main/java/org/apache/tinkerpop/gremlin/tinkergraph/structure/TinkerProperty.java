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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerProperty<V> implements Property<V> {

    protected final Element element;
    protected final String key;
    protected V value;

    public TinkerProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    /**
     * The existence of this object implies the property is present, thus even a {@code null} value means "present".
     */
    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public void remove() {
        if (this.element instanceof Edge) {
            ((AbstractTinkerGraph) this.element.graph()).touch((TinkerEdge) this.element);
            ((TinkerEdge) this.element).properties.remove(this.key);
            TinkerIndexHelper.removeIndex((TinkerEdge) this.element, this.key, this.value);
        } else {
            final TinkerVertex vertex = (TinkerVertex) ((TinkerVertexProperty) this.element).element();
            ((AbstractTinkerGraph) vertex.graph()).touch(vertex);
            ((TinkerVertexProperty) this.element).properties.remove(this.key);
        }
    }

    @Override
    public Object clone() {
        return new TinkerProperty(element, key, value);
    }

    public TinkerProperty copy(final Element newOwner) {
        return new TinkerProperty(newOwner, key, value);
    }
}
