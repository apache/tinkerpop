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
package org.apache.tinkerpop.gremlin.structure.util.keyed;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A utility implementation of a {@link Property} that only has a key but no value and no meta-properties.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class KeyedVertexProperty<V> implements VertexProperty<V> {

    private final String key;

    public KeyedVertexProperty(final String key) {
        if (null == key || key.isEmpty()) throw new IllegalArgumentException("key cannot be null");
        this.key = key;
    }

    @Override
    public Vertex element() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public Object id() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public Graph graph() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public <U> Property<U> property(String key) {
        return Property.<U>empty();
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        return Property.<U>empty();
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public void remove() {

    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        return Collections.emptyIterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final KeyedVertexProperty<?> that = (KeyedVertexProperty<?>) o;

        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
