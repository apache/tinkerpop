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
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a {@link Vertex} that is disconnected from a {@link Graph}.  "Disconnection" can mean detachment from
 * a {@link Graph} in the sense that a {@link Vertex} was constructed from a {@link Graph} instance and this reference
 * was removed or it can mean that the {@code DetachedVertex} could have been constructed independently of a
 * {@link Graph} instance in the first place.
 * <p/>
 * A {@code DetachedVertex} only has reference to the properties that are associated with it at the time of detachment
 * (or construction) and is not traversable or mutable.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DetachedVertex extends DetachedElement<Vertex> implements Vertex {

    private static final String ID = "id";
    private static final String VALUE = "value";
    private static final String PROPERTIES = "properties";

    DetachedVertex() {}

    protected DetachedVertex(final Vertex vertex, final boolean withProperties) {
        super(vertex);

        // only serialize properties if requested, and there are meta properties present. this prevents unnecessary
        // object creation of a new HashMap of a new HashMap which will just be empty.  it will use
        // Collections.emptyMap() by default
        if (withProperties) {
            final Iterator<VertexProperty<Object>> propertyIterator = vertex.properties();
            if (propertyIterator.hasNext()) {
                this.properties = new HashMap<>();
                propertyIterator.forEachRemaining(property -> {
                    final List<Property> list = this.properties.getOrDefault(property.key(), new ArrayList<>());
                    list.add(DetachedFactory.detach(property, true));
                    this.properties.put(property.key(), list);
                });
            }
        }
    }

    public DetachedVertex(final Object id, final String label, final Map<String, Object> properties) {
        super(id, label);
        if (properties != null && !properties.isEmpty()) {
            this.properties = new HashMap<>();
            properties.entrySet().iterator().forEachRemaining(entry ->
                this.properties.put(entry.getKey(), IteratorUtils.<Property>list(IteratorUtils.map(((List<Object>) entry.getValue()).iterator(),
                        m -> VertexProperty.class.isAssignableFrom(m.getClass())
                                ? (VertexProperty) m
                                : new DetachedVertexProperty<>(((Map) m).get(ID), entry.getKey(), ((Map) m).get(VALUE), (Map<String, Object>) ((Map) m).getOrDefault(PROPERTIES, new HashMap<>()), this)))));
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (null != this.properties && this.properties.containsKey(key)) {
            final List<VertexProperty> list = (List) this.properties.get(key);
            if (list.size() > 1)
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return list.get(0);
        } else
            return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw Vertex.Exceptions.edgeAdditionsNotSupported();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return (Iterator) super.properties(propertyKeys);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... labels) {
        return Collections.emptyIterator();
    }

    @Override
    public void remove() {
        throw Vertex.Exceptions.vertexRemovalNotSupported();
    }

    @Override
    void internalAddProperty(final Property p) {
        if (null == properties) properties = new HashMap<>();

        if (!properties.containsKey(p.key()))
            properties.put(p.key(), new ArrayList<>());

        this.properties.get(p.key()).add(p);
    }
}
