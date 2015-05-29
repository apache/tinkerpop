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
package org.apache.tinkerpop.gremlin.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jEntity;

import java.util.Iterator;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class Neo4jElement implements Element, WrappedElement<Neo4jEntity> {
    protected final Neo4jGraph graph;
    protected final Neo4jEntity baseElement;
    protected boolean removed = false;

    public Neo4jElement(final Neo4jEntity baseElement, final Neo4jGraph graph) {
        this.baseElement = baseElement;
        this.graph = graph;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Object id() {
        this.graph.tx().readWrite();
        return this.baseElement.getId();
    }

    @Override
    public Set<String> keys() {
        this.graph.tx().readWrite();
        return Element.super.keys();
    }

    @Override
    public <V> Property<V> property(final String key) {
        this.graph.tx().readWrite();
        try {
            if (this.baseElement.hasProperty(key))
                return new Neo4jProperty<>(this, key, (V) this.baseElement.getProperty(key));
            else
                return Property.empty();
        } catch (final IllegalStateException e) {
            throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());
        }
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();
        try {
            this.baseElement.setProperty(key, value);
            return new Neo4jProperty<>(this, key, value);
        } catch (final IllegalArgumentException e) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
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
    public Neo4jEntity getBaseElement() {
        return this.baseElement;
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        this.graph.tx().readWrite();
        Iterable<String> keys = this.baseElement.getKeys();
        Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                key -> ElementHelper.keyExists(key, propertyKeys));
        return IteratorUtils.map(filter,
                key -> new Neo4jProperty<>(this, key, (V) this.baseElement.getProperty(key)));
    }

}