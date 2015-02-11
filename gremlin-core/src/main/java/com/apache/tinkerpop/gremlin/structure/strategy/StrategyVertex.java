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

import com.apache.tinkerpop.gremlin.structure.Direction;
import com.apache.tinkerpop.gremlin.structure.Edge;
import com.apache.tinkerpop.gremlin.structure.Graph;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import com.apache.tinkerpop.gremlin.structure.VertexProperty;
import com.apache.tinkerpop.gremlin.structure.util.StringFactory;
import com.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyVertex extends StrategyElement implements Vertex, StrategyWrapped, WrappedVertex<Vertex>, Vertex.Iterators {

    private final StrategyContext<StrategyVertex> strategyContext;

    public StrategyVertex(final Vertex baseVertex, final StrategyGraph strategyGraph) {
        super(baseVertex, strategyGraph);
        this.strategyContext = new StrategyContext<>(strategyGraph, this);
    }

    @Override
    public Graph graph() {
        return this.strategyGraph.compose(
                s -> s.getVertexGraphStrategy(this.strategyContext, strategy),
                () -> this.strategyGraph).get();
    }

    @Override
    public Object id() {
        return this.strategyGraph.compose(
                s -> s.getVertexIdStrategy(this.strategyContext, strategy),
                this.getBaseVertex()::id).get();
    }

    @Override
    public String label() {
        return this.strategyGraph.compose(
                s -> s.getVertexLabelStrategy(this.strategyContext, strategy),
                this.getBaseVertex()::label).get();
    }

    @Override
    public Set<String> keys() {
        return this.strategyGraph.compose(
                s -> s.getVertexKeysStrategy(this.strategyContext, strategy),
                this.getBaseVertex()::keys).get();
    }

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.strategyGraph.compose(
                s -> s.<V>getVertexValueStrategy(this.strategyContext, strategy),
                this.getBaseVertex()::value).apply(key);
    }

    @Override
    public void remove() {
        this.strategyGraph.compose(
                s -> s.getRemoveVertexStrategy(this.strategyContext, strategy),
                () -> {
                    this.getBaseVertex().remove();
                    return null;
                }).get();
    }

    @Override
    public Vertex getBaseVertex() {
        return (Vertex) this.baseElement;
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        final Vertex baseInVertex = (inVertex instanceof StrategyVertex) ? ((StrategyVertex) inVertex).getBaseVertex() : inVertex;
        return new StrategyEdge(this.strategyGraph.compose(
                s -> s.getAddEdgeStrategy(this.strategyContext, strategy),
                this.getBaseVertex()::addEdge)
                .apply(label, baseInVertex, keyValues), this.strategyGraph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return new StrategyVertexProperty<>(this.strategyGraph.compose(
                s -> s.<V>getVertexPropertyStrategy(this.strategyContext, strategy),
                this.getBaseVertex()::property).apply(key, value), this.strategyGraph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        return new StrategyVertexProperty<>(this.strategyGraph.compose(
                s -> s.<V>getVertexGetPropertyStrategy(this.strategyContext, strategy),
                this.getBaseVertex()::property).apply(key), this.strategyGraph);
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyElementString(this);
    }

    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        return new StrategyEdge.StrategyEdgeIterator(this.strategyGraph.compose(
                s -> s.getVertexIteratorsEdgeIteratorStrategy(this.strategyContext, strategy),
                (Direction d, String[] l) -> this.getBaseVertex().iterators().edgeIterator(d, l)).apply(direction, edgeLabels), this.strategyGraph);
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... labels) {
        return new StrategyVertexIterator(this.strategyGraph.compose(
                s -> s.getVertexIteratorsVertexIteratorStrategy(strategyContext, strategy),
                (Direction d, String[] l) -> this.getBaseVertex().iterators().vertexIterator(d, l)).apply(direction, labels), this.strategyGraph);
    }

    @Override
    public <V> Iterator<V> valueIterator(final String... propertyKeys) {
        return this.strategyGraph.compose(
                s -> s.<V>getVertexIteratorsValueIteratorStrategy(strategyContext, strategy),
                (String[] pks) -> this.getBaseVertex().iterators().valueIterator(pks)).apply(propertyKeys);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        return IteratorUtils.map(this.strategyGraph.compose(
                        s -> s.<V>getVertexIteratorsPropertyIteratorStrategy(this.strategyContext, strategy),
                        (String[] pks) -> this.getBaseVertex().iterators().propertyIterator(pks)).apply(propertyKeys),
                property -> new StrategyVertexProperty<>(property, this.strategyGraph));
    }

    public static class StrategyVertexIterator implements Iterator<Vertex> {
        private final Iterator<Vertex> vertices;
        private final StrategyGraph strategyGraph;

        public StrategyVertexIterator(final Iterator<Vertex> iterator, final StrategyGraph strategyGraph) {
            this.vertices = iterator;
            this.strategyGraph = strategyGraph;
        }

        @Override
        public boolean hasNext() {
            return this.vertices.hasNext();
        }

        @Override
        public Vertex next() {
            return new StrategyVertex(this.vertices.next(), this.strategyGraph);
        }
    }
}
