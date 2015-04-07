/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.computer.util.star;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StarVertexProperty<V> implements VertexProperty<V> {

    private final Object id;
    private final String key;
    private final V value;
    private Map<String, Object> properties = null;
    private final StarVertex starVertex;

    public StarVertexProperty(final Object id, final String key, final V value, final StarVertex starVertex) {
        this.id = null == id ? StarGraph.randomId() : id;
        this.key = key;
        this.value = value;
        this.starVertex = starVertex;
    }

    @Override
    public String key() {
        return this.key;
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
        return this.starVertex;
    }

    @Override
    public void remove() {
        this.starVertex.properties.get(this.key).remove(this);
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        return null == this.properties ?
                Collections.emptyIterator() :
                (Iterator) this.properties.entrySet().stream()
                        .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                        .map(entry -> new StarProperty<>(entry.getKey(), entry.getValue(), this))
                        .iterator();
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        if (null == this.properties)
            this.properties = new HashMap<>();
        this.properties.put(key, value);
        return new StarProperty<>(key, value, this);
    }

    @Override
    public boolean equals(final Object other) {
        return ElementHelper.areEqual(this, other);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element)this);
    }
}
