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
package org.apache.tinkerpop.gremlin.structure.strategy;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyVertexProperty<V> extends StrategyElement implements VertexProperty<V>, StrategyWrapped, WrappedVertexProperty<VertexProperty<V>>, VertexProperty.Iterators {

    private final StrategyContext<StrategyVertexProperty<V>> strategyContext;

    public StrategyVertexProperty(final VertexProperty<V> baseVertexProperty, final StrategyGraph strategyGraph) {
        super(baseVertexProperty, strategyGraph);
        this.strategyContext = new StrategyContext<>(strategyGraph, this);
    }

    @Override
    public Graph graph() {
        return this.strategyGraph.compose(
                s -> s.getVertexPropertyGraphStrategy(strategyContext, strategy),
                () -> this.strategyGraph).get();
    }

    @Override
    public Object id() {
        return this.strategyGraph.compose(
                s -> s.getVertexPropertyIdStrategy(strategyContext, strategy),
                this.getBaseVertexProperty()::id).get();
    }

    @Override
    public String label() {
        return this.strategyGraph.compose(
                s -> s.getVertexPropertyLabelStrategy(strategyContext, strategy),
                this.getBaseVertexProperty()::label).get();
    }

    @Override
    public Set<String> keys() {
        return this.strategyGraph.compose(
                s -> s.getVertexPropertyKeysStrategy(strategyContext, strategy),
                this.getBaseVertexProperty()::keys).get();
    }

    @Override
    public Vertex element() {
        return new StrategyVertex(this.strategyGraph.compose(
                s -> s.getVertexPropertyGetElementStrategy(strategyContext, strategy),
                this.getBaseVertexProperty()::element).get(), strategyGraph);
    }

    @Override
    public VertexProperty.Iterators iterators() {
        return this;
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        return new StrategyProperty<>(this.strategyGraph.compose(
                s -> s.<U, V>getVertexPropertyPropertyStrategy(strategyContext, strategy),
                this.getBaseVertexProperty()::property).<String, U>apply(key, value), this.strategyGraph);
    }

    @Override
    public String key() {
        return this.strategyGraph.compose(
                s -> s.getVertexPropertyKeyStrategy(this.strategyContext, strategy), this.getBaseVertexProperty()::key).get();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.strategyGraph.compose(
                s -> s.getVertexPropertyValueStrategy(this.strategyContext, strategy), this.getBaseVertexProperty()::value).get();
    }

    @Override
    public boolean isPresent() {
        return this.getBaseVertexProperty().isPresent();
    }

    @Override
    public void remove() {
        this.strategyGraph.compose(
                s -> s.getRemoveVertexPropertyStrategy(this.strategyContext, strategy),
                () -> {
                    this.getBaseVertexProperty().remove();
                    return null;
                }).get();
    }

    @Override
    public VertexProperty<V> getBaseVertexProperty() {
        return (VertexProperty<V>) this.baseElement;
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyElementString(this);
    }


    @Override
    public <U> Iterator<Property<U>> propertyIterator(final String... propertyKeys) {
        return IteratorUtils.map(this.strategyGraph.compose(
                        s -> s.<U, V>getVertexPropertyIteratorsPropertyIteratorStrategy(this.strategyContext, strategy),
                        (String[] pks) -> this.getBaseVertexProperty().iterators().propertyIterator(pks)).apply(propertyKeys),
                property -> new StrategyProperty<>(property, this.strategyGraph));
    }

    @Override
    public <U> Iterator<U> valueIterator(final String... propertyKeys) {
        return this.strategyGraph.compose(
                s -> s.<U, V>getVertexPropertyIteratorsValueIteratorStrategy(this.strategyContext, strategy),
                (String[] pks) -> this.getBaseVertexProperty().iterators().valueIterator(pks)).apply(propertyKeys);
    }
}
