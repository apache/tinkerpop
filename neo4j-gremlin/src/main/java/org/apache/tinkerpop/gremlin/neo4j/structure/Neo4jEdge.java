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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.tinkerpop.api.Neo4jRelationship;

import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Neo4jEdge extends Neo4jElement implements Edge, WrappedEdge<Neo4jRelationship> {

    public Neo4jEdge(final Neo4jRelationship relationship, final Neo4jGraph graph) {
        super(relationship, graph);
    }

    @Override
    public Vertex outVertex() {
        return new Neo4jVertex(this.getBaseEdge().start(), this.graph);
    }

    @Override
    public Vertex inVertex() {
        return new Neo4jVertex(this.getBaseEdge().end(), this.graph);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        this.graph.tx().readWrite();
        switch (direction) {
            case OUT:
                return IteratorUtils.of(new Neo4jVertex(this.getBaseEdge().start(), this.graph));
            case IN:
                return IteratorUtils.of(new Neo4jVertex(this.getBaseEdge().end(), this.graph));
            default:
                return IteratorUtils.of(new Neo4jVertex(this.getBaseEdge().start(), this.graph), new Neo4jVertex(this.getBaseEdge().end(), this.graph));
        }
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        try {
            this.baseElement.delete();
        } catch (IllegalStateException ignored) {
            // NotFoundException happens if the edge is committed
            // IllegalStateException happens if the edge is still chilling in the tx
        } catch (RuntimeException e) {
            if (!Neo4jHelper.isNotFound(e)) throw e;
            // NotFoundException happens if the edge is committed
            // IllegalStateException happens if the edge is still chilling in the tx
        }
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return this.getBaseEdge().type();
    }

    @Override
    public Neo4jRelationship getBaseEdge() {
        return (Neo4jRelationship) this.baseElement;
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        this.graph.tx().readWrite();
        Iterable<String> keys = this.baseElement.getKeys();
        Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                key -> ElementHelper.keyExists(key, propertyKeys));
        return IteratorUtils.map(filter,
                key -> new Neo4jProperty<>(this, key, (V) this.baseElement.getProperty(key)));
    }

    @Override
    public <V> Property<V> property(final String key) {
        this.graph.tx().readWrite();
        if (this.baseElement.hasProperty(key))
            return new Neo4jProperty<>(this, key, (V) this.baseElement.getProperty(key));
        else
            return Property.empty();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();
        try {
            this.baseElement.setProperty(key, value);
            return new Neo4jProperty<>(this, key, value);
        } catch (final IllegalArgumentException e) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value, e);
        }
    }
}