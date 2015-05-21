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
package org.apache.tinkerpop.gremlin.hadoop.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HadoopVertexProperty<V> implements VertexProperty<V>, WrappedVertexProperty<VertexProperty<V>> {

    private final VertexProperty<V> baseVertexProperty;
    private final HadoopVertex hadoopVertex;

    public HadoopVertexProperty(final VertexProperty<V> baseVertexProperty, final HadoopVertex hadoopVertex) {
        this.baseVertexProperty = baseVertexProperty;
        this.hadoopVertex = hadoopVertex;
    }

    @Override
    public Object id() {
        return this.baseVertexProperty.id();
    }

    @Override
    public V value() {
        return this.baseVertexProperty.value();
    }

    @Override
    public String key() {
        return this.baseVertexProperty.key();
    }

    @Override
    public void remove() {
        this.baseVertexProperty.remove();
    }

    @Override
    public boolean isPresent() {
        return this.baseVertexProperty.isPresent();
    }

    @Override
    public <U> Property<U> property(final String key) {
        return this.baseVertexProperty.property(key);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return this.baseVertexProperty.hashCode();
    }

    @Override
    public String toString() {
        return this.baseVertexProperty.toString();
    }

    @Override
    public Vertex element() {
        return this.hadoopVertex;
    }

    @Override
    public VertexProperty<V> getBaseVertexProperty() {
        return this.baseVertexProperty;
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        return IteratorUtils.<Property<U>, Property<U>>map(this.getBaseVertexProperty().properties(propertyKeys), property -> new HadoopProperty<>(property, HadoopVertexProperty.this));
    }
}
