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
package com.tinkerpop.gremlin.hadoop.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopEdge extends HadoopElement implements Edge, Edge.Iterators, WrappedEdge<Edge> {

    protected HadoopEdge() {
    }

    public HadoopEdge(final Edge edge, final HadoopGraph graph) {
        super(edge, graph);
    }

    @Override
    public Edge getBaseEdge() {
        return (Edge) this.baseElement;
    }

    @Override
    public Edge.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.graph.iterators().vertexIterator(getBaseEdge().iterators().vertexIterator(Direction.OUT).next().id())).next();
            case IN:
                return IteratorUtils.of(this.graph.iterators().vertexIterator(getBaseEdge().iterators().vertexIterator(Direction.IN).next().id())).next();
            default: {
                final Iterator<Vertex> iterator = getBaseEdge().iterators().vertexIterator(Direction.BOTH);
                return IteratorUtils.of(this.graph.iterators().vertexIterator(iterator.next().id()).next(), this.graph.iterators().vertexIterator(iterator.next().id()).next());
            }
        }
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return IteratorUtils.<Property<V>, Property<V>>map(this.getBaseEdge().iterators().propertyIterator(propertyKeys), property -> new HadoopProperty<>(property, HadoopEdge.this));
    }
}