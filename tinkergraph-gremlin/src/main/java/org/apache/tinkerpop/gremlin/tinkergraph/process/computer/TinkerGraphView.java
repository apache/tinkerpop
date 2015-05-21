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
package org.apache.tinkerpop.gremlin.tinkergraph.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerGraphView {

    protected final Set<String> computeKeys;
    private Map<Element, Map<String, List<VertexProperty>>> computeProperties;

    public TinkerGraphView(final Set<String> computeKeys) {
        this.computeKeys = computeKeys;
        this.computeProperties = new ConcurrentHashMap<>();
    }

    public <V> Property<V> addProperty(final TinkerVertex vertex, final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        if (isComputeKey(key)) {
            final TinkerVertexProperty<V> property = new TinkerVertexProperty<V>((TinkerVertex) vertex, key, value) {
                @Override
                public void remove() {
                    removeProperty(vertex, key, this);
                }
            };
            this.addValue(vertex, key, property);
            return property;
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
        }
    }

    public List<VertexProperty> getProperty(final TinkerVertex vertex, final String key) {
        return isComputeKey(key) ? this.getValue(vertex, key) : TinkerHelper.getProperties(vertex).getOrDefault(key, Collections.emptyList());
    }

    public List<Property> getProperties(final TinkerVertex vertex) {
        final Stream<Property> a = TinkerHelper.getProperties(vertex).values().stream().flatMap(list -> list.stream());
        final Stream<Property> b = this.computeProperties.containsKey(vertex) ?
                this.computeProperties.get(vertex).values().stream().flatMap(list -> list.stream()) :
                Stream.empty();
        return Stream.concat(a, b).collect(Collectors.toList());
    }

    public void removeProperty(final TinkerVertex vertex, final String key, final VertexProperty property) {
        if (isComputeKey(key)) {
            this.removeValue(vertex, key, property);
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
        }
    }

    //////////////////////

    private void addValue(final Vertex vertex, final String key, final VertexProperty property) {
        final Map<String, List<VertexProperty>> elementProperties = this.computeProperties.computeIfAbsent(vertex, k -> new ConcurrentHashMap<>());
        elementProperties.compute(key, (k, v) -> {
            if (null == v) v = Collections.synchronizedList(new ArrayList<>());
            v.add(property);
            return v;
        });
    }

    private void removeValue(final Vertex vertex, final String key) {
        this.computeProperties.computeIfPresent(vertex, (k, v) -> {
            v.remove(key);
            return v;
        });
    }

    private void removeValue(final Vertex vertex, final String key, final VertexProperty property) {
        this.computeProperties.computeIfPresent(vertex, (k, v) -> {
            v.computeIfPresent(key, (k1, v1) -> {
                v1.remove(property);
                return v1;
            });
            return v;
        });
    }

    private List<VertexProperty> getValue(final Vertex vertex, final String key) {
        return this.computeProperties.getOrDefault(vertex, Collections.emptyMap()).getOrDefault(key, Collections.emptyList());
    }

    public boolean isComputeKey(final String key) {
        return this.computeKeys.contains(key);
    }
}