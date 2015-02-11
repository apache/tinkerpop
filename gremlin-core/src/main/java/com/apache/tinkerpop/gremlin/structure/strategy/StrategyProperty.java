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
package com.apache.tinkerpop.gremlin.structure.strategy;

import com.apache.tinkerpop.gremlin.structure.Edge;
import com.apache.tinkerpop.gremlin.structure.Element;
import com.apache.tinkerpop.gremlin.structure.Property;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import com.apache.tinkerpop.gremlin.structure.util.StringFactory;
import com.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedProperty;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyProperty<V> implements Property<V>, StrategyWrapped, WrappedProperty<Property<V>> {

    private final Property<V> baseProperty;
    private final StrategyContext<StrategyProperty<V>> strategyContext;
    private final StrategyGraph strategyGraph;
    private final GraphStrategy strategy;

    public StrategyProperty(final Property<V> baseProperty, final StrategyGraph strategyGraph) {
        if (baseProperty instanceof StrategyWrapped) throw new IllegalArgumentException(
                String.format("The property %s is already StrategyWrapped and must be a base Property", baseProperty));
        this.baseProperty = baseProperty;
        this.strategyContext = new StrategyContext<>(strategyGraph, this);
        this.strategyGraph = strategyGraph;
        this.strategy = strategyGraph.getStrategy();
    }

    @Override
    public String key() {
        return this.strategyGraph.compose(
                s -> s.getPropertyKeyStrategy(this.strategyContext, strategy), this.baseProperty::key).get();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.strategyGraph.compose(
                s -> s.getPropertyValueStrategy(this.strategyContext, strategy), this.baseProperty::value).get();
    }

    @Override
    public boolean isPresent() {
        return this.baseProperty.isPresent();
    }

    @Override
    public Element element() {
        final Element baseElement = this.baseProperty.element();
        return (baseElement instanceof Vertex ? new StrategyVertex((Vertex) baseElement, this.strategyGraph) :
                new StrategyEdge((Edge) baseElement, this.strategyGraph));
    }

    @Override
    public <E extends Throwable> V orElseThrow(final Supplier<? extends E> exceptionSupplier) throws E {
        return this.baseProperty.orElseThrow(exceptionSupplier);
    }

    @Override
    public V orElseGet(final Supplier<? extends V> valueSupplier) {
        return this.baseProperty.orElseGet(valueSupplier);
    }

    @Override
    public V orElse(final V otherValue) {
        return this.baseProperty.orElse(otherValue);
    }

    @Override
    public void ifPresent(final Consumer<? super V> consumer) {
        this.baseProperty.ifPresent(consumer);
    }

    @Override
    public void remove() {
        this.strategyGraph.compose(
                s -> s.getRemovePropertyStrategy(strategyContext, strategy),
                () -> {
                    this.baseProperty.remove();
                    return null;
                }).get();
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyPropertyString(this);
    }

    @Override
    public Property<V> getBaseProperty() {
        return this.baseProperty;
    }
}
