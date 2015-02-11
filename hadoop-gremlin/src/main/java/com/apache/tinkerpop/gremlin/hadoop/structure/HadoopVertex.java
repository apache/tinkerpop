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
package com.apache.tinkerpop.gremlin.hadoop.structure;

import com.apache.tinkerpop.gremlin.structure.Direction;
import com.apache.tinkerpop.gremlin.structure.Edge;
import com.apache.tinkerpop.gremlin.structure.Element;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import com.apache.tinkerpop.gremlin.structure.VertexProperty;
import com.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import com.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopVertex extends HadoopElement implements Vertex, Vertex.Iterators, WrappedVertex<Vertex> {

    protected HadoopVertex() {
    }

    public HadoopVertex(final Vertex vertex, final HadoopGraph graph) {
        super(vertex, graph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        final VertexProperty<V> vertexProperty = getBaseVertex().<V>property(key);
        return vertexProperty.isPresent() ?
                new HadoopVertexProperty<>((VertexProperty<V>) ((Vertex) this.baseElement).property(key), this) :
                VertexProperty.<V>empty();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw Vertex.Exceptions.edgeAdditionsNotSupported();
    }

    @Override
    public Vertex getBaseVertex() {
        return (Vertex) this.baseElement;
    }

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... edgeLabels) {
        return IteratorUtils.map(this.getBaseVertex().iterators().vertexIterator(direction, edgeLabels), vertex -> HadoopVertex.this.graph.iterators().vertexIterator(vertex.id()).next());
    }

    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        return IteratorUtils.map(this.getBaseVertex().iterators().edgeIterator(direction, edgeLabels), edge -> HadoopVertex.this.graph.iterators().edgeIterator(edge.id()).next());
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        return IteratorUtils.<VertexProperty<V>, VertexProperty<V>>map(this.getBaseVertex().iterators().propertyIterator(propertyKeys), property -> new HadoopVertexProperty<>(property, HadoopVertex.this));
    }
}
