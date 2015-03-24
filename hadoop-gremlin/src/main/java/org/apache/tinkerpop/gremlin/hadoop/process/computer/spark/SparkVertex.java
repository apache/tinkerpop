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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark;

import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkVertex implements Vertex, Serializable {

    // TODO: Wrapped vertex -- need VertexProgram in partition (broadcast variable?)

    private final VertexWritable vertexWritable;

    public SparkVertex(final TinkerVertex vertex) {
        this.vertexWritable = new VertexWritable(vertex);
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        return this.vertexWritable.get().addEdge(label, inVertex, keyValues);
    }

    @Override
    public Object id() {
        return this.vertexWritable.get().id();
    }

    @Override
    public String label() {
        return this.vertexWritable.get().label();
    }

    @Override
    public Graph graph() {
        return this.vertexWritable.get().graph();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        return this.vertexWritable.get().property(cardinality, key, value, keyValues);
    }

    @Override
    public void remove() {
        this.vertexWritable.get().remove();
    }


    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return this.vertexWritable.get().edges(direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return this.vertexWritable.get().vertices(direction, edgeLabels);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return this.vertexWritable.get().properties(propertyKeys);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this.vertexWritable.get());
    }

    @Override
    public int hashCode() {
        return this.vertexWritable.get().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return ElementHelper.areEqual(this.vertexWritable.get(), other);
    }
}
