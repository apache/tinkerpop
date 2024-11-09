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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Represents an {@link Edge} that is disconnected from a {@link Graph}.  "Disconnection" can mean detachment from
 * a {@link Graph} in the sense that the {@link Edge} was constructed from a {@link Graph} instance and this reference
 * was removed or it can mean that the {@code DetachedEdge} could have been constructed independently of a
 * {@link Graph} instance in the first place.
 * <p/>
 * A {@code DetachedEdge} only has reference to the properties and in/out vertices that are associated with it at the
 * time of detachment (or construction) and is not traversable or mutable.  Note that the references to the in/out
 * vertices are {@link DetachedVertex} instances that only have reference to the
 * {@link org.apache.tinkerpop.gremlin.structure.Vertex#id()} and {@link org.apache.tinkerpop.gremlin.structure.Vertex#label()}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedEdge extends DetachedElement<Edge> implements Edge {

    private DetachedVertex outVertex;
    private DetachedVertex inVertex;

    private DetachedEdge() {}

    protected DetachedEdge(final Edge edge, final boolean withProperties) {
        super(edge);
        this.outVertex = DetachedFactory.detach(edge.outVertex(), false);
        this.inVertex = DetachedFactory.detach(edge.inVertex(), false);

        // only serialize properties if requested, the graph supports it and there are meta properties present.
        // this prevents unnecessary object creation of a new HashMap of a new HashMap which will just be empty.
        // it will use Collections.emptyMap() by default
        if (withProperties) {
            final Iterator<Property<Object>> propertyIterator = edge.properties();
            if (propertyIterator.hasNext()) {
                this.properties = new HashMap<>();
                propertyIterator.forEachRemaining(property -> this.properties.put(property.key(), Collections.singletonList(DetachedFactory.detach(property))));
            }
        }
    }

    public DetachedEdge(final Object id, final String label,
                        final Map<String, Object> properties,
                        final Object outVId, final String outVLabel,
                        final Object inVId, final String inVLabel) {
        super(id, label);
        this.outVertex = new DetachedVertex(outVId, outVLabel, Collections.emptyMap());
        this.inVertex = new DetachedVertex(inVId, inVLabel, Collections.emptyMap());
        if (properties != null && !properties.isEmpty()) {
            this.properties = new HashMap<>();
            properties.entrySet().iterator().forEachRemaining(entry -> {
                if (Property.class.isAssignableFrom(entry.getValue().getClass())) {
                    this.properties.put(entry.getKey(), Collections.singletonList((Property)entry.getValue()));
                } else {
                    this.properties.put(entry.getKey(), Collections.singletonList(new DetachedProperty<>(entry.getKey(), entry.getValue(), this)));
                }
            });
        }
    }

    public DetachedEdge(final Object id, final String label,
                        final List<Property> properties,
                        final Object outVId, final String outVLabel,
                        final Object inVId, final String inVLabel) {
        super(id, label);
        this.outVertex = new DetachedVertex(outVId, outVLabel, Collections.emptyMap());
        this.inVertex = new DetachedVertex(inVId, inVLabel, Collections.emptyMap());
        if (properties != null && !properties.isEmpty()) {
            this.properties = new HashMap<>();
            properties.iterator().forEachRemaining(property -> {
                if (!this.properties.containsKey(property.key())) {
                    this.properties.put(property.key(), new ArrayList());
                }
                this.properties.get(property.key()).add(property);
            });
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Vertex inVertex() {
        return this.inVertex;
    }

    @Override
    public Vertex outVertex() {
        return this.outVertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex);
            case IN:
                return IteratorUtils.of(this.inVertex);
            default:
                return IteratorUtils.of(this.outVertex, this.inVertex);
        }
    }

    @Override
    public void remove() {
        throw Edge.Exceptions.edgeRemovalNotSupported();
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return (Iterator) super.properties(propertyKeys);
    }

    @Override
    void internalAddProperty(final Property p) {
        if (null == properties) properties = new HashMap<>();
        this.properties.put(p.key(), Collections.singletonList(p));
    }

    /**
     * Provides a way to construct an immutable {@link DetachedEdge}.
     */
    public static DetachedEdge.Builder build() {
        return new Builder(new DetachedEdge());
    }

    public static class Builder {
        private DetachedEdge e;

        private Builder(final DetachedEdge e) {
            this.e = e;
        }

        public Builder addProperty(final Property p) {
            e.internalAddProperty(p);
            return this;
        }

        public Builder setId(final Object id) {
            e.id = id;
            return this;
        }

        public Builder setLabel(final String label) {
            e.label = label;
            return this;
        }

        public Builder setOutV(final DetachedVertex v) {
            e.outVertex = v;
            return this;
        }

        public Builder setInV(final DetachedVertex v) {
            e.inVertex = v;
            return this;
        }

        public DetachedEdge create() {
            return e;
        }
    }
}
