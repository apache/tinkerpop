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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerEdge extends TinkerElement implements Edge {

    protected Map<String, Property> properties;

    protected Vertex inVertex = null;
    protected Object inVertexId = null;
    protected Vertex outVertex = null;
    protected Object outVertexId= null;
    private final AbstractTinkerGraph graph;
    private final boolean allowNullPropertyValues;
    private final boolean isTxMode;

    protected TinkerEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex) {
       this(id, outVertex, label, inVertex, 0);
    }

    protected TinkerEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex, final long currentVersion) {
        this(id, (AbstractTinkerGraph) outVertex.graph(), outVertex.id(), label, inVertex.id(), currentVersion, false);
        if (!isTxMode) {
            this.inVertex = inVertex;
            this.outVertex = outVertex;
        }
    }

    private TinkerEdge(final Object id, AbstractTinkerGraph graph, final Object outVertexId, final String label, final Object inVertexId, final long currentVersion, final Boolean skipIndexUpdate) {
        super(id, label, currentVersion);
        isTxMode = graph instanceof TinkerTransactionGraph;
        this.graph = graph;
        if (isTxMode) {
            this.outVertexId = outVertexId;
            this.inVertexId = inVertexId;
        }
        this.allowNullPropertyValues = graph.features().edge().supportsNullPropertyValues();
        if (!skipIndexUpdate)
            TinkerIndexHelper.autoUpdateIndex(this, T.label.getAccessor(), this.label, null);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        graph.touch(this);

        if (this.removed) throw elementAlreadyRemoved(Edge.class, id);
        ElementHelper.validateProperty(key, value);

        if (!allowNullPropertyValues && null == value) {
            properties(key).forEachRemaining(Property::remove);
            return Property.empty();
        }

        final Property oldProperty = super.property(key);
        final Property<V> newProperty = new TinkerProperty<>(this, key, value);
        if (null == this.properties) this.properties = new ConcurrentHashMap<>();
        this.properties.put(key, newProperty);
        TinkerIndexHelper.autoUpdateIndex(this, key, value, oldProperty.isPresent() ? oldProperty.value() : null);
        return newProperty;
    }

    @Override
    public <V> Property<V> property(final String key) {
        return null == this.properties ? Property.<V>empty() : this.properties.getOrDefault(key, Property.<V>empty());
    }

    @Override
    public Set<String> keys() {
        return null == this.properties ? Collections.emptySet() : this.properties.keySet();
    }

    @Override
    public void remove() {
        graph.touch(this);
        TinkerIndexHelper.removeElementIndex(this);
        graph.removeEdge(this.id());
        this.properties = null;
        this.removed = true;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Object clone() {
        if (!isTxMode) {
            // shallow copy for non-tx mode
            final TinkerEdge edge = new TinkerEdge(id, outVertex, label, inVertex, currentVersion);
            edge.properties = properties;
            return edge;
        }

        final TinkerEdge edge = new TinkerEdge(id, graph, outVertexId, label, inVertexId, currentVersion, true);

        if (properties != null) {
            final Map<String, Property> cloned = new ConcurrentHashMap<>(properties.size());
            properties.entrySet().stream().forEach(p -> cloned.put(p.getKey(), ((TinkerProperty) p.getValue()).copy(edge)));

            edge.properties = cloned;
        }

        return edge;
    }

    @Override
    public Vertex outVertex() {
        return isTxMode ? graph.vertex(outVertexId) : outVertex;
    }

    @Override
    public Vertex inVertex() {
        return isTxMode ? graph.vertex(inVertexId) : inVertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        if (removed) return Collections.emptyIterator();
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex());
            case IN:
                return IteratorUtils.of(this.inVertex());
            default:
                return IteratorUtils.of(this.outVertex(), this.inVertex());
        }
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        if (null == this.properties) return Collections.emptyIterator();
        if (propertyKeys.length == 1) {
            if (null == propertyKeys[0])
                return Collections.emptyIterator();
            final Property<V> property = this.properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else
            return (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).map(entry -> entry.getValue()).collect(Collectors.toList()).iterator();
    }
}
