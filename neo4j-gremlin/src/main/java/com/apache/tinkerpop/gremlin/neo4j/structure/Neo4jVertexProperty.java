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
package com.apache.tinkerpop.gremlin.neo4j.structure;

import com.apache.tinkerpop.gremlin.process.T;
import com.apache.tinkerpop.gremlin.structure.Element;
import com.apache.tinkerpop.gremlin.structure.Graph;
import com.apache.tinkerpop.gremlin.structure.Property;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import com.apache.tinkerpop.gremlin.structure.VertexProperty;
import com.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import com.apache.tinkerpop.gremlin.structure.util.StringFactory;
import com.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jVertexProperty<V> implements VertexProperty<V>, VertexProperty.Iterators, WrappedVertex<Node> {

    public static final Label VERTEX_PROPERTY_LABEL = DynamicLabel.label("vertexProperty");
    public static final String VERTEX_PROPERTY_PREFIX = Graph.Hidden.hide("");
    public static final String VERTEX_PROPERTY_TOKEN = Graph.Hidden.hide("vertexProperty");


    private Node node;
    private final Neo4jVertex vertex;
    private final String key;
    private final V value;


    public Neo4jVertexProperty(final Neo4jVertex vertex, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        this.node = null;
    }

    public Neo4jVertexProperty(final Neo4jVertex vertex, final Node node) {
        this.vertex = vertex;
        this.node = node;
        this.key = (String) node.getProperty(T.key.getAccessor());
        this.value = (V) node.getProperty(T.value.getAccessor());
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public Object id() {
        // TODO: Neo4j needs a better ID system for VertexProperties
        return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public Node getBaseVertex() {
        return this.node;
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        if (!this.vertex.graph.supportsMetaProperties)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();

        ElementHelper.validateProperty(key, value);
        this.vertex.graph.tx().readWrite();
        if (isNode()) {
            this.node.setProperty(key, value);
            return new Neo4jProperty<>(this, key, value);
        } else {
            this.node = this.vertex.graph.getBaseGraph().createNode(VERTEX_PROPERTY_LABEL, DynamicLabel.label(this.label()));
            this.node.setProperty(T.key.getAccessor(), this.key);
            this.node.setProperty(T.value.getAccessor(), this.value);
            this.node.setProperty(key, value);
            this.vertex.getBaseVertex().createRelationshipTo(this.node, DynamicRelationshipType.withName(VERTEX_PROPERTY_PREFIX.concat(this.key)));
            this.vertex.getBaseVertex().setProperty(this.key, VERTEX_PROPERTY_TOKEN);
            return new Neo4jProperty<>(this, key, value);
        }
    }

    @Override
    public <U> Property<U> property(final String key) {
        if (!this.vertex.graph.supportsMetaProperties)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();

        this.vertex.graph.tx().readWrite();
        try {
            if (this.node.hasProperty(key))
                return new Neo4jProperty<>(this, key, (U) this.node.getProperty(key));
            else
                return Property.empty();
        } catch (IllegalStateException | NotFoundException ex) {
            throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());
        }
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
    public Set<String> keys() {
        if (!this.vertex.graph.supportsMetaProperties)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();

        if (isNode()) {
            this.vertex.graph.tx().readWrite();
            final Set<String> keys = new HashSet<>();
            for (final String key : this.node.getPropertyKeys()) {
                if (!Graph.Hidden.isHidden(key))
                    keys.add(key);
            }
            return keys;
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public void remove() {
        this.vertex.graph.tx().readWrite();
        if (!this.vertex.graph.supportsMetaProperties) {
            if (this.vertex.getBaseVertex().hasProperty(this.key))
                this.vertex.getBaseVertex().removeProperty(this.key);
        } else {
            if (isNode()) {
                this.node.getRelationships().forEach(Relationship::delete);
                this.node.delete();
                if (this.vertex.getBaseVertex().getDegree(DynamicRelationshipType.withName(VERTEX_PROPERTY_PREFIX.concat(this.key)), Direction.OUTGOING) == 0) {
                    if (this.vertex.getBaseVertex().hasProperty(this.key))
                        this.vertex.getBaseVertex().removeProperty(this.key);
                }
            } else {
                if (this.vertex.getBaseVertex().getDegree(DynamicRelationshipType.withName(VERTEX_PROPERTY_PREFIX.concat(this.key)), Direction.OUTGOING) == 0) {
                    if (this.vertex.getBaseVertex().hasProperty(this.key))
                        this.vertex.getBaseVertex().removeProperty(this.key);
                }
            }
        }
    }

    private boolean isNode() {
        return null != this.node;
    }

    @Override
    public VertexProperty.Iterators iterators() {
        if (!this.vertex.graph.supportsMetaProperties)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        else
            return this;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public <U> Iterator<Property<U>> propertyIterator(final String... propertyKeys) {
        if (!isNode()) return Collections.emptyIterator();
        else {
            this.vertex.graph().tx().readWrite();
            return IteratorUtils.map(IteratorUtils.filter(this.node.getPropertyKeys().iterator(), key -> !key.equals(T.key.getAccessor()) && !key.equals(T.value.getAccessor()) && ElementHelper.keyExists(key, propertyKeys)), key -> (Property<U>) new Neo4jProperty<>(Neo4jVertexProperty.this, key, (V) this.node.getProperty(key)));
        }
    }
}
