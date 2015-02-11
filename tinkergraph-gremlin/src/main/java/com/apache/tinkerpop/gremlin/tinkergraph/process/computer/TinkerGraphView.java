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
package com.apache.tinkerpop.gremlin.tinkergraph.process.computer;

import com.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import com.apache.tinkerpop.gremlin.structure.Element;
import com.apache.tinkerpop.gremlin.structure.Property;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import com.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import com.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;
import com.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;
import com.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import com.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import com.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;

import java.util.ArrayList;
import java.util.Arrays;
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
public class TinkerGraphView {

    protected final Set<String> computeKeys;
    protected final GraphComputer.Isolation isolation;
    private Map<Element, Map<String, List<Property>>> computeProperties;

    public TinkerGraphView(final GraphComputer.Isolation isolation, final Set<String> computeKeys) {
        this.isolation = isolation;
        this.computeKeys = computeKeys;
        this.computeProperties = new ConcurrentHashMap<>();
    }

    public <V> Property<V> setProperty(final TinkerElement element, final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        if (isComputeKey(key)) {
            if (element instanceof Vertex) {
                final TinkerVertexProperty<V> property = new TinkerVertexProperty<V>((TinkerVertex) element, key, value) {
                    @Override
                    public void remove() {
                        removeProperty(element, key, this);
                    }
                };
                this.setValue(element, key, property);
                return property;
            } else {
                final TinkerProperty<V> property = new TinkerProperty<V>(element, key, value) {
                    @Override
                    public void remove() {
                        removeProperty((TinkerElement) element, key, this);
                    }
                };
                this.setValue(element, key, property);
                return property;
            }
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
        }
    }

    public List<Property> getProperty(final TinkerElement element, final String key) {
        return isComputeKey(key) ? this.getValue(element, key) : TinkerHelper.getProperties(element).getOrDefault(key, Collections.emptyList());
    }

    public List<Property> getProperties(final TinkerElement element) {
        final Stream<Property> a = TinkerHelper.getProperties(element).values().stream().flatMap(list -> list.stream());
        final Stream<Property> b = this.computeProperties.containsKey(element) ?
                this.computeProperties.get(element).values().stream().flatMap(list -> list.stream()) :
                Stream.empty();
        return Stream.concat(a, b).collect(Collectors.toList());
    }

    public void removeProperty(final TinkerElement element, final String key, final Property property) {
        if (isComputeKey(key)) {
            if (element instanceof Vertex)
                this.removeValue(element, key, property);
            else
                this.removeValue(element, key);
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
        }
    }

    //////////////////////

    private void setValue(final Element element, final String key, final Property property) {
        final Map<String, List<Property>> elementProperties = this.computeProperties.computeIfAbsent(element, k -> new ConcurrentHashMap<>());
        elementProperties.compute(key, (k, v) -> {
            if (element instanceof Vertex) {
                if (null == v) v = Collections.synchronizedList(new ArrayList<>());
                v.add(property);
            } else
                v = Arrays.asList(property);
            return v;
        });
    }

    private void removeValue(final Element element, final String key) {
        this.computeProperties.computeIfPresent(element, (k, v) -> {
            v.remove(key);
            return v;
        });
    }

    private void removeValue(final Element element, final String key, final Property property) {
        this.computeProperties.computeIfPresent(element, (k, v) -> {
            v.computeIfPresent(key, (k1, v1) -> {
                v1.remove(property);
                return v1;
            });
            return v;
        });
    }

    private List<Property> getValue(final Element element, final String key) {
        return this.computeProperties.getOrDefault(element, Collections.emptyMap()).getOrDefault(key, Collections.emptyList());
    }

    public boolean isComputeKey(final String key) {
        return this.computeKeys.contains(key);
    }
}