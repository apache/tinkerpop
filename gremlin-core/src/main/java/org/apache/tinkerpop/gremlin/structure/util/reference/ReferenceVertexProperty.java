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
package org.apache.tinkerpop.gremlin.structure.util.reference;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceVertexProperty<V> extends ReferenceElement<VertexProperty<V>> implements VertexProperty<V> {

    private ReferenceVertex vertex;
    private V value;

    private ReferenceVertexProperty() {

    }

    public ReferenceVertexProperty(final VertexProperty<V> vertexProperty) {
        super(vertexProperty);
        this.vertex = null == vertexProperty.element() ? null : ReferenceFactory.detach(vertexProperty.element());
        this.value = vertexProperty.value();
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public String key() {
        return this.label;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public void remove() {
        throw Property.Exceptions.propertyRemovalNotSupported();
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        return Collections.emptyIterator();
    }
}
